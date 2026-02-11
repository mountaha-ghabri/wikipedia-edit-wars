"""
Wikipedia Data Collector - Optimized for High-Volume Collection
"""


import requests
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import random


class WikipediaDataCollector:
    def __init__(self, config_path="/opt/spark/project/config/api_config.json"):
        """Initialize with configuration"""
       
        with open(config_path) as f:
            self.config = json.load(f)
       
        self.api_config = self.config['wikipedia_api']
        self.data_config = self.config['data_collection']
       
        self.base_url = self.api_config['base_url']
        self.user_agent = self.api_config['user_agent']
        self.rate_limit = 0.1  # Reduced from 0.5 to 1.0 - be aggressive
       
        # Reduce concurrency to avoid 429s
        self.max_workers = 2  # Down from 8
       
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
       
        self.output_dir = Path("/opt/spark/project/data/bronze/wikipedia")
        self.output_dir.mkdir(parents=True, exist_ok=True)


        try:
            import pandas as _pd
            import pyarrow as _pyarrow
            self._pd = _pd
            self._pyarrow = _pyarrow
            self.parquet_enabled = True
        except Exception:
            self.parquet_enabled = False
       
        print("="*80)
        print("WIKIPEDIA EDIT WARS - DATA COLLECTOR (OPTIMIZED)")
        print("="*80)
        print(f"Target: {self.data_config['target_records']:,} records")
        print(f"Output: {self.output_dir}")
        print("="*80)
   
    def api_call_with_retry(self, params, max_retries=5):
        """Make API call with aggressive backoff"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=30
                )


                if response.status_code == 429:
                    retry_after = response.headers.get('Retry-After', str(2 ** attempt + random.uniform(1, 3)))
                    wait_time = float(retry_after)
                    print(f"   ⚠️  429 - sleeping {wait_time:.1f}s")
                    time.sleep(wait_time)
                    continue


                response.raise_for_status()
                return response.json()


            except requests.exceptions.Timeout:
                wait_time = 2 ** attempt + random.uniform(0.5, 1.5)
                print(f"   ⚠️  Timeout, retry {attempt + 1}/{max_retries}, sleep {wait_time:.1f}s")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                    continue
                return None
               
            except Exception as e:
                wait_time = 2 ** attempt + random.uniform(0.5, 1.5)
                if attempt < max_retries - 1:
                    print(f"   ⚠️  Error: {type(e).__name__}, retry in {wait_time:.1f}s")
                    time.sleep(wait_time)
                else:
                    print(f"   ❌ Failed after {max_retries} retries")
                    return None
       
        return None
   
    def get_category_pages(self, category, limit=500):
        """Get pages from category with pagination"""
        all_pages = []
        cmcontinue = None
        attempts = 0
       
        while len(all_pages) < limit and attempts < 5:
            params = {
                'action': 'query',
                'list': 'categorymembers',
                'cmtitle': f'Category:{category}',
                'cmlimit': min(500, limit - len(all_pages)),
                'cmtype': 'page',
                'format': 'json'
            }
           
            if cmcontinue:
                params['cmcontinue'] = cmcontinue
           
            data = self.api_call_with_retry(params)
            if not data:
                break
           
            pages = data.get('query', {}).get('categorymembers', [])
            all_pages.extend([p['title'] for p in pages])
           
            if 'continue' in data and len(all_pages) < limit:
                cmcontinue = data['continue'].get('cmcontinue')
            else:
                break
           
            attempts += 1
            time.sleep(self.rate_limit)
       
        return all_pages
   
    def get_page_revisions(self, title, limit=500):
        """Get revisions with retry on failure"""
        all_revisions = []
        rvcontinue = None
        attempts = 0
       
        while len(all_revisions) < limit and attempts < 3:
            params = {
                'action': 'query',
                'titles': title,
                'prop': 'revisions|info',
                'rvprop': 'ids|timestamp|user|userid|size|comment|flags|tags',
                'rvlimit': min(500, limit - len(all_revisions)),
                'inprop': 'watchers',
                'format': 'json'
            }
           
            if rvcontinue:
                params['rvcontinue'] = rvcontinue
           
            data = self.api_call_with_retry(params)
            if not data:
                break
           
            pages = data.get('query', {}).get('pages', {})
           
            for page_id, page_data in pages.items():
                if page_id == '-1':
                    continue
               
                page_watchers = page_data.get('watchers', 0)
                revisions = page_data.get('revisions', [])
               
                for rev in revisions:
                    size = rev.get('size', 0)
                    revision_record = {
                        'page_title': title,
                        'page_id': page_id,
                        'page_watchers': page_watchers,
                        'revid': rev.get('revid'),
                        'parentid': rev.get('parentid', 0),
                        'timestamp': rev.get('timestamp'),
                        'user': rev.get('user', 'Unknown'),
                        'userid': rev.get('userid', 0),
                        'anon': 'anon' in rev,
                        'size': size,
                        'comment': rev.get('comment', ''),
                        'minor': 'minor' in rev,
                        'bot': 'bot' in rev,
                        'tags': rev.get('tags', []),
                        'collected_at': datetime.now(timezone.utc).isoformat()
                    }
                    all_revisions.append(revision_record)
           
            if 'continue' in data and len(all_revisions) < limit:
                rvcontinue = data['continue'].get('rvcontinue')
            else:
                break
           
            attempts += 1
            time.sleep(self.rate_limit)
       
        return all_revisions
   
    def save_batch(self, records, batch_num):
        """Save batch atomically"""
        filename = f"wikipedia_edits_batch_{batch_num:05d}.json"
        filepath = self.output_dir / filename
        json_tmp = self.output_dir / (filename + ".tmp")
       
        with open(json_tmp, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False)
        json_tmp.replace(filepath)
        print(f"   💾 Batch {batch_num}: {len(records):,} records")
   
    def _scan_existing_batches(self):
        """Resume from existing batches"""
        total = 0
        max_batch = -1
        for p in sorted(self.output_dir.iterdir()):
            if p.name.endswith('.json') and 'wikipedia_edits_batch_' in p.name:
                try:
                    with open(p, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        cnt = len(data) if isinstance(data, list) else 0
                        total += cnt
                        num = int(p.name.split('_')[-1].split('.')[0])
                        if num > max_batch:
                            max_batch = num
                except Exception:
                    pass
       
        return total, max_batch + 1
   
    def collect_dataset(self):
        """Main collection loop"""
       
        target = self.data_config['target_records']
        batch_size = self.data_config['batch_size']
        categories = self.data_config['categories']
       
        total_collected = 0
        batch_num = 0
        current_batch = []
       
        start_time = datetime.now()
       
        print(f"\n🚀 Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
       
        existing_total, existing_batch_num = self._scan_existing_batches()
        if existing_total > 0:
            total_collected = existing_total
            batch_num = existing_batch_num
            print(f"🔁 Resuming: {total_collected:,} records, batch {batch_num}\n")


        for cat_idx, category in enumerate(categories, 1):
            if total_collected >= target:
                break
           
            print(f"\n{'='*80}")
            print(f"CATEGORY {cat_idx}/{len(categories)}: {category}")
            print(f"Progress: {total_collected:,} / {target:,} ({total_collected/target*100:.1f}%)")
            print(f"{'='*80}")
           
            print(f"📚 Fetching pages...")
            pages = self.get_category_pages(category, limit=500)
            print(f"   ✅ Found {len(pages)} pages\n")
           
            if not pages:
                continue
           
            # Process with minimal concurrency
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self.get_page_revisions, title, 500): title for title in pages}


                for i, future in enumerate(as_completed(futures), 1):
                    if total_collected >= target:
                        break
                   
                    title = futures[future]
                    try:
                        revisions = future.result()
                    except Exception as e:
                        print(f"   ⚠️  {title}: {type(e).__name__}")
                        revisions = []


                    if revisions:
                        current_batch.extend(revisions)
                        total_collected += len(revisions)
                       
                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = total_collected / elapsed if elapsed > 0 else 0
                        remaining = (target - total_collected) / rate if rate > 0 else 0
                       
                        print(f"📄 [{i}/{len(pages)}] {title} | +{len(revisions)} | "
                              f"⏱️ {rate:.0f} rec/s | ETA {remaining/3600:.1f}h")


                        if len(current_batch) >= batch_size:
                            self.save_batch(current_batch, batch_num)
                            current_batch = []
                            batch_num += 1
       
        if current_batch:
            self.save_batch(current_batch, batch_num)
       
        end_time = datetime.now()
        duration = end_time - start_time
       
        print("\n" + "="*80)
        print(f"✅ COMPLETE: {total_collected:,} records / {duration}")
        print("="*80)
       
        return total_collected



def main():
    try:
        collector = WikipediaDataCollector()
        total = collector.collect_dataset()
        target = collector.data_config.get('target_records', 10_000_000)
        sys.exit(0 if total >= int(target) else 1)
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)



if __name__ == "__main__":
    main()