
import os
import gc
import glob
import re
import json
import argparse
import pandas as pd
import torch
import torch.nn.functional as F
from pathlib import Path
from peft import PeftConfig, PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
from tqdm import tqdm

# Default policy file path (relative to this script)
DEFAULT_POLICY_FILE = Path(__file__).parent / "tiktok_policy.txt"


def load_prompt_template(policy_file=None):
    """Load the prompt template from a file.

    Args:
        policy_file: Path to the policy file. If None, uses DEFAULT_POLICY_FILE.

    Returns:
        The prompt template string with {content_text} placeholder.

    Raises:
        FileNotFoundError: If the policy file does not exist.
    """
    if policy_file is None:
        policy_file = DEFAULT_POLICY_FILE

    policy_path = Path(policy_file)
    if not policy_path.exists():
        raise FileNotFoundError(f"Policy file not found: {policy_path}")

    with open(policy_path, 'r') as f:
        return f.read()

def find_date_based_parquet_files(root_dir):
    """Find parquet files with date-based naming convention.
    
    Args:
        root_dir: Directory to search for parquet files
        start_date: Start date as string 'YYYY-MM-DD' (optional)
        end_date: End date as string 'YYYY-MM-DD' (optional)
    
    Returns:
        List of matching parquet file paths
    """
    parquet_files = []
    
    # Pattern to match files like "0_2025-01-31_00_00_subtitles.parquet"
    pattern = os.path.join(root_dir, "*_????-??-??_??_??_subtitles.parquet")
    
    # Find all matching files
    all_files = glob.glob(pattern)
    
    # Extract date from filename and filter by date range if provided
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
    
    for file_path in all_files:
        filename = os.path.basename(file_path)
        match = date_pattern.search(filename)
        
        if match:
            file_date = match.group(1)
            
            # Check if file date is within specified range
            include_file = True
            
            # if start_date and file_date < start_date:
            #     include_file = False
            # if end_date and file_date > end_date:
            #     include_file = False
                
            if include_file:
                parquet_files.append(file_path)
                print(f"Found: {file_path}")
            else:
                print(f"Skipped (outside date range): {file_path}")
        else:
            print(f"Warning: Could not extract date from filename: {filename}")
    
    # Sort files by the extracted date
    parquet_files.sort(key=lambda x: date_pattern.search(os.path.basename(x)).group(1) if date_pattern.search(os.path.basename(x)) else '')
    
    return parquet_files

def extract_date_from_filename(filename):
    """Extract year, month, and day from filename.
    
    Args:
        filename: Filename like "0_2025-01-31_00_00_subtitles.parquet"
    
    Returns:
        Tuple of (year, month, day) as integers, or (None, None, None) if not found
    """
    date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
    match = date_pattern.search(filename)
    
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        return year, month, day
    else:
        return None, None, None

def get_checkpoint_path(output_file):
    """Get checkpoint file path based on output file."""
    return Path(output_file).with_suffix('.checkpoint.parquet')


def save_checkpoint(results, checkpoint_path, processed_indices):
    """Save current results and processed indices to checkpoint."""
    df = pd.DataFrame(results)
    df.to_parquet(checkpoint_path, index=False)
    # Also save processed indices to a companion file
    indices_path = checkpoint_path.with_suffix('.indices.json')
    with open(indices_path, 'w') as f:
        json.dump(list(processed_indices), f)


def load_checkpoint(checkpoint_path):
    """Load results and processed indices from checkpoint."""
    if not checkpoint_path.exists():
        return [], set()
    df = pd.read_parquet(checkpoint_path)
    results = df.to_dict('records')
    indices_path = checkpoint_path.with_suffix('.indices.json')
    if indices_path.exists():
        with open(indices_path, 'r') as f:
            processed_indices = set(json.load(f))
    else:
        processed_indices = set()
    return results, processed_indices


def load_and_combine_parquets(parquet_files):
    """Load and combine multiple parquet files into a single dataframe.

    Also extracts date information from filenames and adds year, month, day columns.
    """
    dfs = []
    
    for file_path in parquet_files:
        try:
            df = pd.read_parquet(file_path)
            
            # Extract date information from filename
            filename = os.path.basename(file_path)
            year, month, day = extract_date_from_filename(filename)
            
            # Add date columns to the dataframe
            df['year'] = year
            df['month'] = month
            df['day'] = day
            df['source_filename'] = filename  # Keep track of source file
            
            print(f"Loaded {len(df)} rows from {file_path} (date: {year}-{month:02d}-{day:02d})")
            dfs.append(df)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"Combined {len(parquet_files)} files into {len(combined_df)} total rows")
        return combined_df
    else:
        return pd.DataFrame()




def process_subtitles_directory(input_dir, output_file=None, prompt_template=None, tokenizer=None, model=None, device=None, batch_size=16, checkpoint_interval=1000, resume=False):
    """Process all subtitle parquet files in a directory and run classification.

    Args:
        input_dir: Directory containing subtitle parquet files
        output_file: Path to save results CSV (optional)
        prompt_template: Prompt template to use for classification
        tokenizer: Tokenizer instance
        model: Model instance
        device: Device (cuda/cpu)
        batch_size: Number of texts to process in each batch
        checkpoint_interval: Save checkpoint every N records (default: 1000)
        resume: Resume from existing checkpoint if available

    Returns:
        DataFrame with classification results
    """
    if prompt_template is None:
        prompt_template = load_prompt_template()
    
    # Find all subtitle parquet files
    print(f"Searching for subtitle files in: {input_dir}")
    parquet_files = find_date_based_parquet_files(input_dir)
    
    if not parquet_files:
        print("No subtitle files found!")
        return pd.DataFrame()
    
    print(f"Found {len(parquet_files)} subtitle files")
    
    # Load and combine all parquet files
    combined_df = load_and_combine_parquets(parquet_files)
    
    if combined_df.empty:
        print("No data loaded from parquet files!")
        return pd.DataFrame()
    
    print(f"Loaded {len(combined_df)} total subtitle records")
    
    # Check if 'content' column exists
    if 'content' not in combined_df.columns:
        print(f"Error: 'content' column not found in data. Available columns: {combined_df.columns.tolist()}")
        return pd.DataFrame()
    
    # Filter out rows with missing content
    original_count = len(combined_df)
    combined_df = combined_df.dropna(subset=['content'])
    filtered_count = len(combined_df)
    
    if filtered_count < original_count:
        print(f"Filtered out {original_count - filtered_count} rows with missing content")
    
    # Setup checkpointing
    checkpoint_path = get_checkpoint_path(output_file) if output_file else None
    processed_indices = set()
    results = []

    if resume and checkpoint_path and checkpoint_path.exists():
        results, processed_indices = load_checkpoint(checkpoint_path)
        print(f"Resumed from checkpoint: {len(results)} records already processed")

    # Run classification in batches for efficiency
    print(f"Running classification with batch size {batch_size}...")

    # Prepare data for batch processing
    valid_rows = []
    valid_contents = []

    for idx, row in combined_df.iterrows():
        content = row['content']
        if isinstance(content, str) and content.strip():
            valid_rows.append((idx, row))
            valid_contents.append(content)

    # Filter out already-processed items when resuming
    if processed_indices:
        items_to_process = [(idx, row, content) for (idx, row), content
                           in zip(valid_rows, valid_contents)
                           if idx not in processed_indices]
        print(f"Skipping {len(valid_rows) - len(items_to_process)} already processed items")
    else:
        items_to_process = [(idx, row, content) for (idx, row), content
                           in zip(valid_rows, valid_contents)]

    print(f"Processing {len(items_to_process)} valid subtitle entries...")

    if items_to_process:
        # Extract contents for batch processing
        batch_indices = [item[0] for item in items_to_process]
        batch_rows = [item[1] for item in items_to_process]
        batch_contents = [item[2] for item in items_to_process]

        try:
            # Process in batches with checkpointing
            for i in tqdm(range(0, len(batch_contents), batch_size), desc="Processing batches"):
                batch_slice_contents = batch_contents[i:i+batch_size]
                batch_slice_indices = batch_indices[i:i+batch_size]
                batch_slice_rows = batch_rows[i:i+batch_size]

                # Process this batch
                predictions = predict_batch(batch_slice_contents, prompt_template, tokenizer, model, device, batch_size)

                # Combine predictions with row data
                for idx, row, prediction in zip(batch_slice_indices, batch_slice_rows, predictions):
                    result = {
                        'index': idx,
                        'year': row.get('year'),
                        'month': row.get('month'),
                        'day': row.get('day'),
                        'source_filename': row.get('source_filename'),
                        'content': row['content'],
                        'decoded_token': prediction['decoded_token'],
                        'classification': prediction['classification'],
                        'prob_0': prediction['prob_0'],
                        'prob_1': prediction['prob_1']
                    }
                    results.append(result)
                    processed_indices.add(idx)

                # Save checkpoint periodically
                if checkpoint_path and len(results) % checkpoint_interval < batch_size:
                    save_checkpoint(results, checkpoint_path, processed_indices)
                    print(f"Checkpoint saved: {len(results)} records")

        except Exception as e:
            print(f"Error during batch processing: {e}")
            # Save checkpoint before falling back
            if checkpoint_path and results:
                save_checkpoint(results, checkpoint_path, processed_indices)
                print(f"Checkpoint saved before fallback: {len(results)} records")
            print("Falling back to individual processing...")

            # Get remaining items to process
            remaining_items = [(idx, row, content) for idx, row, content in items_to_process
                              if idx not in processed_indices]

            # Fallback to individual processing if batch fails
            for idx, row, content in tqdm(remaining_items, desc="Fallback processing"):
                try:
                    prediction = predict(content, prompt_template, tokenizer, model, device)

                    result = {
                        'index': idx,
                        'year': row.get('year'),
                        'month': row.get('month'),
                        'day': row.get('day'),
                        'source_filename': row.get('source_filename'),
                        'content': content,
                        'decoded_token': prediction['decoded_token'],
                        'classification': prediction['classification'],
                        'prob_0': prediction['prob_0'],
                        'prob_1': prediction['prob_1']
                    }
                    results.append(result)
                    processed_indices.add(idx)

                    # Save checkpoint periodically in fallback mode too
                    if checkpoint_path and len(results) % checkpoint_interval < 1:
                        save_checkpoint(results, checkpoint_path, processed_indices)
                        print(f"Checkpoint saved: {len(results)} records")

                except Exception as row_error:
                    print(f"Error processing row {idx}: {row_error}")
                    continue

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    if not results_df.empty:
        print(f"Classification complete! Processed {len(results_df)} items")
        print(f"Classification summary:")
        print(f"  - Class 0 (no labels apply): {len(results_df[results_df['classification'] == 0])} ({len(results_df[results_df['classification'] == 0])/len(results_df)*100:.1f}%)")
        print(f"  - Class 1 (labels apply): {len(results_df[results_df['classification'] == 1])} ({len(results_df[results_df['classification'] == 1])/len(results_df)*100:.1f}%)")

        # Save results if output file specified
        if output_file:
            results_df.to_parquet(output_file, index=False)
            print(f"Results saved to: {output_file}")

    # Clean up checkpoint files after successful completion
    if checkpoint_path and checkpoint_path.exists():
        checkpoint_path.unlink()
        indices_path = checkpoint_path.with_suffix('.indices.json')
        if indices_path.exists():
            indices_path.unlink()
        print("Checkpoint files cleaned up after successful completion")

    return results_df

# Function to make predictions in batches for efficiency
def predict_batch(contents, prompt_template, tokenizer, model, device, batch_size=16):
    """Process multiple texts in batches for improved GPU utilization.

    Args:
        contents: List of content strings to classify
        prompt_template: Prompt template with {content_text} placeholder
        tokenizer: Tokenizer instance
        model: Model instance
        device: Device (cuda/cpu)
        batch_size: Number of texts to process in each batch

    Returns:
        List of prediction dictionaries
    """
    results = []
    
    # Pre-compute token IDs for '0' and '1' to avoid repeated encoding
    token_0_id = tokenizer.encode('0', add_special_tokens=False)[0]
    token_1_id = tokenizer.encode('1', add_special_tokens=False)[0]
    
    # Process contents in batches
    for i in range(0, len(contents), batch_size):
        batch_contents = contents[i:i+batch_size]
        
        # Create input texts for the batch
        batch_input_texts = [prompt_template.format(content_text=content) for content in batch_contents]
        
        # Tokenize batch with padding
        batch_inputs = tokenizer(
            batch_input_texts, 
            return_tensors="pt", 
            padding=True, 
            truncation=True, 
            max_length=2048  # Reasonable limit for gemma-2-9b
        ).to(device)
        
        with torch.inference_mode():
            outputs = model(**batch_inputs)
            
            # Get logits for the last token in each sequence
            # For padded sequences, we need the last non-pad token
            batch_size_actual = outputs.logits.shape[0]
            seq_lengths = batch_inputs.attention_mask.sum(dim=1) - 1  # -1 for 0-indexing
            
            # Extract logits for the last token of each sequence
            last_token_logits = outputs.logits[range(batch_size_actual), seq_lengths, :]
            
            # Apply softmax to get probabilities
            probabilities = F.softmax(last_token_logits, dim=-1)
            
            # Get predicted token IDs
            predicted_token_ids = torch.argmax(last_token_logits, dim=-1)
            
            # Process each item in the batch
            for j in range(batch_size_actual):
                predicted_token_id = predicted_token_ids[j].item()
                decoded_output = tokenizer.decode([predicted_token_id])
                
                prob_0 = probabilities[j, token_0_id].item()
                prob_1 = probabilities[j, token_1_id].item()
                
                result = {
                    'decoded_token': decoded_output,
                    'classification': 1 if decoded_output == '1' else 0,
                    'prob_0': prob_0,
                    'prob_1': prob_1
                }
                results.append(result)

        # Clear GPU cache after each batch to prevent memory fragmentation
        del batch_inputs, outputs, last_token_logits, probabilities, predicted_token_ids
        torch.cuda.empty_cache()
        gc.collect()

    return results

# Legacy single prediction function for backwards compatibility
def predict(content, prompt_template, tokenizer, model, device):
    """Single text prediction - uses batch function with batch_size=1."""
    results = predict_batch([content], prompt_template, tokenizer, model, device, batch_size=1)
    return results[0] if results else None

def main():
    parser = argparse.ArgumentParser(description="Classify TikTok subtitle content using experimental classifier")
    parser.add_argument("--input_dir", help="Directory containing subtitle parquet files")
    parser.add_argument("-o", "--output", help="Output parquet file for results")
    parser.add_argument("--policy", help=f"Path to custom policy file (defaults to {DEFAULT_POLICY_FILE})")
    parser.add_argument("--batch-size", type=int, default=4, help="Batch size for GPU processing (default: 4)")
    parser.add_argument("--checkpoint-interval", type=int, default=1000,
                        help="Save checkpoint every N records (default: 1000)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from existing checkpoint if available")

    args = parser.parse_args()

    # Load prompt template from policy file
    policy_file = args.policy if args.policy else None
    prompt_template = load_prompt_template(policy_file)
    policy_source = args.policy if args.policy else DEFAULT_POLICY_FILE
    print(f"Loaded prompt template from: {policy_source}")
    
    # Set default output filename if not specified
    output_file = args.output
    if not output_file:
        output_file = "classification_results.parquet"
    
    print(f"Processing subtitle files from: {args.input_dir}")
    print(f"Output will be saved to: {output_file}")
    
    # Classification starts here
    device = 'cuda' if torch.cuda.is_available() else 'cpu'

    # Local model paths (mounted in container at /models)
    base_model_path = "/models/gemma-2-9b"
    adapter_model_path = "/models/cope-a-adapter"

    bnb_config = BitsAndBytesConfig(
        load_in_8bit=True,
    )

    print(f"Loading base model from: {base_model_path}")
    model = AutoModelForCausalLM.from_pretrained(base_model_path,
                                                quantization_config=bnb_config,
                                                device_map="auto",
                                                local_files_only=True)
    print(f"Loading adapter from: {adapter_model_path}")
    model = PeftModel.from_pretrained(model, adapter_model_path, local_files_only=True)
    model = model.merge_and_unload()

    tokenizer = AutoTokenizer.from_pretrained(base_model_path, local_files_only=True)

    # Process the directory with batch processing and checkpointing
    results_df = process_subtitles_directory(
        args.input_dir, output_file, prompt_template, tokenizer, model, device,
        args.batch_size, args.checkpoint_interval, args.resume
    )
    
    if not results_df.empty:
        print("\nClassification completed successfully!")
    else:
        print("\nNo results generated. Check input directory and file format.")

if __name__ == "__main__":
    main()
