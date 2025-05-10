import json
from typing import Dict, Any, Set
from main import log_event, current_timestamp
 
TRADE_LEDGER_PATH = "trade_ledger.json"
EXAMPLE_TXT_PATH = "trade_speculations/example.txt"

def load_json_file(filename: str) -> Dict[str, Any]:
    """Load JSON data from a file."""
    with open(filename, 'r') as f:
        return json.load(f)

def extract_json_from_text(filename: str) -> Dict[str, Any]:
    """Extract JSON object from a text file that may contain other content."""
    with open(filename, 'r') as f:
        content = f.read()
    
    # Find the start and end of the JSON object
    start = content.find('{')
    end = content.rfind('}') + 1
    
    if start == -1 or end == 0:
        raise ValueError("No JSON object found in the file")
    
    json_str = content[start:end]
    return json.loads(json_str)

def get_all_keys(data: Dict[str, Any], prefix: str = '') -> Set[str]:
    """Recursively get all keys from a dictionary with dot notation for nested keys."""
    keys = set()
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            keys.update(get_all_keys(value, full_key))
        else:
            keys.add(full_key)
    return keys

def add_missing_fields(target: Dict[str, Any], template: Dict[str, Any], path: str = '') -> None:
    """
    Recursively add missing fields from template to target.
    Uses dot notation path for nested keys.
    """
    for key, value in template.items():
        current_path = f"{path}.{key}" if path else key
        if key not in target:
            # Set type-appropriate null values
            if isinstance(value, dict):
                target[key] = {}
            elif isinstance(value, list):
                target[key] = []
            elif isinstance(value, (int, float)):
                target[key] = 0
            elif isinstance(value, bool):
                target[key] = False
            elif isinstance(value, str):
                target[key] = ""
            else:
                target[key] = None
        elif isinstance(value, dict) and isinstance(target[key], dict):
            add_missing_fields(target[key], value, current_path)

def sync_trade_schema() -> None:
    """Main function to sync trade schema between template and ledger."""
    # Initialize logging
    log_event({
        "script": "sync_trade_schema",
        "action": "process_started"
    })
    
    # Load the files
    try:
        ledger = load_json_file(TRADE_LEDGER_PATH)
        template = extract_json_from_text(EXAMPLE_TXT_PATH)

        # Convert template values to null-defined types
        null_template = {}
        for key, value in template.items():
            if isinstance(value, dict):
                null_template[key] = {}
            elif isinstance(value, list):
                null_template[key] = []
            elif isinstance(value, (int, float)):
                null_template[key] = 0
            elif isinstance(value, bool):
                null_template[key] = False
            elif isinstance(value, str):
                null_template[key] = ""
            else:
                null_template[key] = None
    except Exception as e:
        log_event({
            "script": "sync_trade_schema",
            "action": "load_failed",
            "error": str(e)
        })
        return

    # Get all keys from template
    template_keys = get_all_keys(null_template)
    synced_trades = []
    
    # Update each trade in the ledger
    for trade_id, trade_data in ledger.items():
        # Get keys from current trade
        trade_keys = get_all_keys(trade_data)
        
        # Find missing keys
        missing_keys = template_keys - trade_keys
        
        if missing_keys:
            # Add missing fields
            add_missing_fields(trade_data, null_template)
            synced_trades.append(trade_id)
            
            # Log added fields for this trade
            log_event({
                "script": "sync_trade_schema",
                "action": "fields_added",
                "trade_id": trade_id,
                "added_fields": sorted(missing_keys)
            })
    
    # Save the updated ledger
    try:
        with open(TRADE_LEDGER_PATH, 'w') as f:
            json.dump(ledger, f, indent=2)
        
        # Log summary of synced trades
        log_event({
            "script": "sync_trade_schema",
            "action": "process_completed",
            "trades_synced": len(synced_trades),
            "synced_trade_ids": synced_trades
        })
    except Exception as e:
        log_event({
            "script": "sync_trade_schema",
            "action": "save_failed",
            "error": str(e)
        })

if __name__ == '__main__':
    sync_trade_schema()