import os
import json

# Your specific path
TARGET_PATH = r"C:\Users\HP\Desktop\sleeptrial\data\bronze\wikipedia"
GOAL = 10_000_000

def count_records(directory):
    total = 0
    file_count = 0
    if not os.path.exists(directory):
        print(f"❌ Directory not found: {directory}")
        return 0, 0
    
    # Get all json files
    files = [f for f in os.listdir(directory) if f.endswith(".json")]
    
    for filename in files:
        file_count += 1
        path = os.path.join(directory, filename)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Count list items (batch) or single objects
                total += len(data) if isinstance(data, list) else 1
        except Exception:
            continue
            
    return total, file_count

current_total, total_files = count_records(TARGET_PATH)
percent = (current_total / GOAL) * 100
remaining = max(0, GOAL - current_total)

# Progress bar visual
bar_length = 20
filled_length = int(bar_length * current_total // GOAL)
bar = '█' * filled_length + '-' * (bar_length - filled_length)

print("\n" + "="*40)
print(f"📊 WIKIPEDIA COLLECTION STATUS")
print("="*40)
print(f"📂 Folder:      ...\\data\\bronze\\wikipedia")
print(f"📄 Files:       {total_files:,} JSON batches")
print(f"✅ Collected:   {current_total:,} records")
print(f"🎯 Goal:        {GOAL:,} records")
print("-" * 40)
print(f"📈 Progress:    [{bar}] {percent:.2f}%")
print(f"🚩 Remaining:   {remaining:,} records")
print("="*40 + "\n")