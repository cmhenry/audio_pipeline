
import os
import glob
import re
import argparse
import pandas as pd
import torch
import torch.nn.functional as F
from peft import PeftConfig, PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
from tqdm import tqdm

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




PROMPT = """
INSTRUCTIONS
============
Examine the given POLICY and determine if the given CONTENT meets the criteria for ANY of the LABELS. Answer "1" if yes, and "0" if no.

POLICY
======

{policy}


CONTENT
=======

{content}


ANSWER
======

"""

DEFAULT_POLICY = """# Policy

## Overview

This policy is designed to determine whether or not content is abusive speech.

## Definition of Terms

- Individual Target: Any person or specific group of people who are directly addressed or clearly identifiable as the subject of the content.
- Public Figure: Individuals who have achieved prominence in society through their actions, positions, or public presence, including politicians, celebrities, business leaders, and social media influencers with substantial followings.
- Threatening Language: Words or phrases that express intent to cause harm, intimidate, or coerce someone through violence, exposure of private information, or other means of causing distress.
- Degrading Language: Words or phrases intended to humiliate, belittle, or diminish a person's dignity or self-worth.
- Private Individual: Any person who is not a public figure and has not voluntarily placed themselves in public discourse on the matter being discussed.

## Interpretation of Language:

- In interpreting this policy, context and tone should be considered. Harsh criticism of ideas, actions, or public positions should be distinguished from personal attacks on individuals. The severity and directness of the language should be weighed against the public nature of the discourse.

## Definition of Labels

### (AS): Abusive Speech

#### Includes

- Direct Threats and Intimidation: Any expression that threatens physical harm, death, sexual violence, or other forms of violence against specific individuals or clearly defined groups, regardless of whether the threat is credible.
- Severe Personal Attacks: Content containing cruel, degrading, or dehumanizing language directed at specific individuals, including but not limited to attacks on appearance, intelligence, character, or personal circumstances.
- Harassment and Bullying: Persistent, targeted communication intended to intimidate, humiliate, or cause emotional distress to individuals, including coordinated harassment campaigns or pile-ons.
- Doxxing and Privacy Violations: Sharing or threatening to share private information about individuals with intent to cause harm, enable harassment, or violate privacy.
- Sexual Harassment: Unwanted sexual advances, explicit sexual content directed at individuals without consent, or sexually degrading language aimed at specific people.
- Malicious Impersonation: Creating content that falsely represents someone else with intent to damage their reputation or cause them harm.
- Targeted Profanity and Vulgarity: Use of extreme profanity, obscenities, or vulgar language when directed aggressively at specific individuals.
- Encouraging Self-Harm: Content that encourages, instructs, or goads individuals to harm themselves or end their lives.
- Coordinated Abuse: Content that explicitly calls for others to harass, abuse, or target specific individuals.

#### Excludes

- Criticism of Ideas and Actions: Strong criticism, even harsh criticism, of ideas, beliefs, actions, decisions, or public positions, provided it does not include personal attacks or threats.
- Public Figure Criticism: Robust criticism, satire, or mockery of public figures regarding their public actions, statements, or positions, unless it includes threats of violence or crosses into severe personal attacks unrelated to their public role.
- Group Criticism Without Individual Targeting: Criticism of organizations, institutions, corporations, or abstract groups where no specific individuals are targeted for abuse.
- Self-Defense and Counter-Speech: Content where individuals defend themselves against attacks or respond proportionally to abuse directed at them.
- Factual Reporting: Neutral reporting of events, crimes, or misconduct, even when such reporting may be unflattering to the subjects.
- Academic or Educational Discussion: Content that discusses abusive behavior in an educational, analytical, or documentary context without endorsing or encouraging such behavior.
- Consensual Exchanges: Apparently harsh exchanges between individuals where context suggests mutual consent or participation in debate, roasting, or similar activities.
- Profanity Not Directed at Individuals: Use of profanity for emphasis, frustration with situations, or general expression that is not directed at specific people.
- Fiction and Creative Content: Abusive language that appears within clearly marked fictional content, quotes from literary works, or artistic expression where real individuals are not targeted.
"""

DEFAULT_CONTENT = "Put your content sample here."

def process_subtitles_directory(input_dir, output_file=None, policy=None, tokenizer=None, model=None, device=None, batch_size=16):
    """Process all subtitle parquet files in a directory and run classification.
    
    Args:
        input_dir: Directory containing subtitle parquet files
        output_file: Path to save results CSV (optional)
        policy: Classification policy to use (defaults to DEFAULT_POLICY)
    
    Returns:
        DataFrame with classification results
    """
    if policy is None:
        policy = DEFAULT_POLICY
    
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
    
    # Run classification in batches for efficiency
    print(f"Running classification with batch size {batch_size}...")
    results = []
    
    # Prepare data for batch processing
    valid_rows = []
    valid_contents = []
    
    for idx, row in combined_df.iterrows():
        content = row['content']
        if isinstance(content, str) and content.strip():
            valid_rows.append((idx, row))
            valid_contents.append(content)
    
    print(f"Processing {len(valid_contents)} valid subtitle entries...")
    
    if valid_contents:
        try:
            # Process all contents in batches
            predictions = predict_batch(valid_contents, policy, tokenizer, model, device, batch_size)
            
            # Combine predictions with row data
            for (idx, row), prediction in tqdm(
                zip(valid_rows, predictions), 
                total=len(valid_rows), 
                desc="Processing results"
            ):
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
                
        except Exception as e:
            print(f"Error during batch processing: {e}")
            print("Falling back to individual processing...")
            
            # Fallback to individual processing if batch fails
            for idx, row in tqdm(valid_rows, desc="Fallback processing"):
                try:
                    content = row['content']
                    prediction = predict(content, policy, tokenizer, model, device)
                    
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
    
    return results_df

# Function to make predictions in batches for efficiency
def predict_batch(contents, policy, tokenizer, model, device, batch_size=16):
    """Process multiple texts in batches for improved GPU utilization.
    
    Args:
        contents: List of content strings to classify
        policy: Policy text to use for classification
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
        batch_input_texts = [PROMPT.format(policy=policy, content=content) for content in batch_contents]
        
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
    
    return results

# Legacy single prediction function for backwards compatibility
def predict(content, policy, tokenizer, model, device):
    """Single text prediction - uses batch function with batch_size=1."""
    results = predict_batch([content], policy, tokenizer, model, device, batch_size=1)
    return results[0] if results else None

def main():
    parser = argparse.ArgumentParser(description="Classify TikTok subtitle content using experimental classifier")
    parser.add_argument("--input_dir", help="Directory containing subtitle parquet files")
    parser.add_argument("-o", "--output", help="Output parquet file for results")
    parser.add_argument("--policy", help="Path to custom policy file (defaults to built-in policy)")
    parser.add_argument("--batch-size", type=int, default=16, help="Batch size for GPU processing (default: 16)")
    
    args = parser.parse_args()
    
    # Load custom policy if specified
    policy = DEFAULT_POLICY
    if args.policy and os.path.exists(args.policy):
        with open(args.policy, 'r') as f:
            policy = f.read()
        print(f"Loaded custom policy from: {args.policy}")
    
    # Set default output filename if not specified
    output_file = args.output
    if not output_file:
        output_file = "classification_results.parquet"
    
    print(f"Processing subtitle files from: {args.input_dir}")
    print(f"Output will be saved to: {output_file}")
    
    # Classification starts here
    device = 'cuda' if torch.cuda.is_available() else 'cpu'

    base_model_name = "google/gemma-2-9b"
    adapter_model_name = "zentropi-ai/cope-a-9b"

    bnb_config = BitsAndBytesConfig(
        load_in_8bit=True,
    )

    model = AutoModelForCausalLM.from_pretrained(base_model_name,
                                                token=os.environ['HF_TOKEN'],
                                                quantization_config=bnb_config,
                                                device_map="auto")
    model = PeftModel.from_pretrained(model, adapter_model_name, token=os.environ['HF_TOKEN'])
    model = model.merge_and_unload()

    #model = model.to(device)

    tokenizer = AutoTokenizer.from_pretrained(base_model_name)

    # Process the directory with batch processing
    results_df = process_subtitles_directory(
        args.input_dir, output_file, policy, tokenizer, model, device, args.batch_size
    )
    
    if not results_df.empty:
        print("\nClassification completed successfully!")
    else:
        print("\nNo results generated. Check input directory and file format.")

if __name__ == "__main__":
    main()
