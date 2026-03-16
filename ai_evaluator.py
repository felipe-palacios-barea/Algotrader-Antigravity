"""
Module: ai_evaluator.py

Fetches enriched_trades.csv (which includes all trade + match info), queries the Google Gemini API to assess
whether a given congress member's stock trade is related to their committee/subcommittee,
appends a probability (0–1) plus a brief comment. Only fills empty or missing values in
'probability' and 'comment' columns (leaves existing results intact).
Persists back to enriched_trades.csv.
"""
import json
import pandas as pd
import time
from openai import OpenAI
from config import OPENAI_API_KEY, MODEL_NAME, ENRICHED_TRADES_CSV_PATH

# Initialize OpenAI Client
client = OpenAI(api_key=OPENAI_API_KEY)

# Polite pause between API calls (removed for paid tier)
PAUSE_SEC = 0.0


def ask_relation(investor: str, ticker: str, committee: str, subcommittee: str, high_amt: float) -> dict:
    """
    Queries the OpenAI model to score the relationship between a stock and a committee, along with politician alpha and conviction.
    Returns a dict with keys: 'probability', 'lobbying_impact', 'trump_interest', 'politician_alpha', 'trade_size_conviction' (floats 0-1) and 'comment' (str).
    """
    prompt = (
        "Given the following:\n"
        f"- Congress member: {investor}\n"
        f"- Stock ticker: {ticker}\n"
        f"- Committee: {committee}\n"
        f"- Subcommittee: {subcommittee}\n"
        f"- High Amount traded in USD: {high_amt}\n\n"
        "Using your knowledge of the committees the congress member is on, the typical topics, legislation, and meetings discussed in those committees/subcommittees, and the nature of the business for the stock invested in, evaluate five factors:\n"
        "1. How likely it is that the committee would discuss topics that may affect the business in a meaningful way.\n"
        "2. How meaningful the contributions and lobbying efforts of this business (and its broader industry) are within that specific committee.\n"
        "3. The probability of Donald Trump's positive interest in the company based on his current political proposals, relationships with important people in the company or broader industry, political agenda, and other relevant factors.\n"
        "4. The conviction of the trade by mapping the High Amount traded against the congressperson's estimated net worth.\n"
        "Please respond with valid JSON containing exactly five keys:\n"
        '1. "probability": a float between 0.0 and 1.0 indicating the probability of the company being affected by the committee/subcommittee.\n'
        '2. "lobbying_impact": a float between 0.0 and 1.0 indicating how meaningful the contributions/lobbying of the business and industry are in that committee.\n'
        '3. "trump_interest": a float between 0.0 and 1.0 indicating the probability of Donald Trump\'s positive interest in the company based on his political proposals, agenda, and relationships.\n'
        '4. "trade_size_conviction": a float between 0.0 and 1.0 indicating the conviction of the trade relative to their estimated net worth.\n'
        '5. "comment": a brief one-sentence or two-sentence explanation justifying all scores, combining policy impact, lobbying, Trump factor, and trade size.'
    )
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                response_format={ "type": "json_object" },
                messages=[
                    {"role": "system", "content": "You are a financial and political data analyst. You always output valid JSON exactly as requested."},
                    {"role": "user", "content": prompt}
                ]
            )
            raw = response.choices[0].message.content.strip()
            
            # Strip markdown fences if present
            if raw.startswith("```"):
                lines = raw.split("\n")
                if lines[-1].strip() == "```":
                    lines = lines[:-1]
                if lines[0].startswith("```"):
                    lines = lines[1:]
                raw = "\n".join(lines).strip()
                
            # Extract JSON substring
            start, end = raw.find("{"), raw.rfind("}")
            json_str = raw[start:end+1] if start != -1 and end != -1 else raw
            
            data = json.loads(json_str)
            prob = float(data.get('probability', 0))
            lob_impact = float(data.get('lobbying_impact', 0))
            trump_interest = float(data.get('trump_interest', 0))
            trade_conviction = float(data.get('trade_size_conviction', 0))
            comment = data.get('comment', '').strip()
            return {
                'probability': prob, 
                'lobbying_impact': lob_impact, 
                'trump_interest': trump_interest, 
                'trade_size_conviction': trade_conviction,
                'comment': comment
            }
            
        except Exception as e:
            err_str = str(e)
            if "429" in err_str and attempt < max_retries - 1:
                print(f"Rate limit hit for {ticker}. Waiting 65 seconds to clear quota before retrying (Attempt {attempt + 1}/{max_retries})...")
                time.sleep(65)
                continue
            
            prob = None
            lob_impact = None
            trump_interest = None
            trade_conviction = None
            comment = f"Error or invalid JSON: {err_str}"
            print(f"Failed parsing response for {ticker}: {comment}")
            return {'probability': prob, 'lobbying_impact': lob_impact, 'trump_interest': trump_interest, 'trade_size_conviction': trade_conviction, 'comment': comment}
            
    return {'probability': None, 'lobbying_impact': None, 'trump_interest': None, 'trade_size_conviction': None, 'comment': "Exceeded max retries."}


def evaluate_trades():
    """
    Load enriched_trades.csv, call ask_relation on each row only if 'probability' and 'comment' are empty,
    append results, and save back.
    """
    try:
        df = pd.read_csv(ENRICHED_TRADES_CSV_PATH, encoding='utf-8-sig')
    except FileNotFoundError:
        print(f"File not found: {ENRICHED_TRADES_CSV_PATH}")
        return

    # Map the empirical politician alpha
    from pathlib import Path
    metrics_path = Path(__file__).parent / 'politician_alpha_metrics.csv'
    if metrics_path.exists():
        alpha_df = pd.read_csv(metrics_path)
        alpha_map = dict(zip(alpha_df['Investor Name'], alpha_df['Alpha_Score_0_to_1']))
        df['politician_alpha'] = df['Investor Name'].map(alpha_map)
    else:
        df['politician_alpha'] = pd.NA

    # Ensure output columns exist
    if 'probability' not in df.columns:
        df['probability'] = pd.NA
    if 'lobbying_impact' not in df.columns:
        df['lobbying_impact'] = pd.NA
    if 'trump_interest' not in df.columns:
        df['trump_interest'] = pd.NA
    if 'politician_alpha' not in df.columns:
        df['politician_alpha'] = pd.NA
    if 'trade_size_conviction' not in df.columns:
        df['trade_size_conviction'] = pd.NA
    if 'comment' not in df.columns:
        df['comment'] = pd.NA

    updated_count = 0
    for idx, row in df.iterrows():
        prob_val = row['probability']
        lob_val = row.get('lobbying_impact', pd.NA)
        trump_val = row.get('trump_interest', pd.NA)
        alpha_val = row.get('politician_alpha', pd.NA)
        convict_val = row.get('trade_size_conviction', pd.NA)
        comment_val = row['comment']
        
        # Check for non-empty
        has_prob = pd.notna(prob_val) and str(prob_val).strip().lower() not in ['', 'nan']
        has_lob = pd.notna(lob_val) and str(lob_val).strip().lower() not in ['', 'nan']
        has_trump = pd.notna(trump_val) and str(trump_val).strip().lower() not in ['', 'nan']
        has_alpha = pd.notna(alpha_val) and str(alpha_val).strip().lower() not in ['', 'nan']
        has_convict = pd.notna(convict_val) and str(convict_val).strip().lower() not in ['', 'nan']
        has_comment = pd.notna(comment_val) and str(comment_val).strip().lower() not in ['', 'nan']
        
        if has_prob and has_lob and has_trump and has_alpha and has_convict and has_comment:
            continue
            
        # Fill missing
        inv = row.get('Investor Name', '')
        ticker = row.get('Ticker', '')
        committee = row.get('Committees', '')
        subcommittee = row.get('Subcommittees', '')
        high_amt = float(row.get('High Amount', 0)) if pd.notna(row.get('High Amount')) else 0.0

        # Skip if no real ticker info
        if pd.isna(ticker) or str(ticker).strip() == '':
            continue
            
        res = ask_relation(inv, ticker, committee, subcommittee, high_amt)
        df.at[idx, 'probability'] = res['probability']
        df.at[idx, 'lobbying_impact'] = res['lobbying_impact']
        df.at[idx, 'trump_interest'] = res['trump_interest']
        df.at[idx, 'trade_size_conviction'] = res['trade_size_conviction']
        df.at[idx, 'comment'] = res['comment']
        updated_count += 1
        time.sleep(PAUSE_SEC)

    if updated_count > 0:
        df.to_csv(ENRICHED_TRADES_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"Updated {ENRICHED_TRADES_CSV_PATH} — evaluated {updated_count} new rows.")
    else:
        print("No new rows needed evaluation.")


if __name__ == '__main__':
    evaluate_trades()
