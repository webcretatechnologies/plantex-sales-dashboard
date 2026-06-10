import os
import sys
import django

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pltx_dashboard.settings')
django.setup()

from apps.dashboard.models import AsinFsnMapping  # noqa: E402
from apps.accounts.models import Users  # noqa: E402
import openpyxl  # noqa: E402

def run():
    user = Users.objects.first()
    if not user:
        print("No users found")
        return
        
    print(f"Loading mapping for user {user.email}...")
    wb = openpyxl.load_workbook('../FSN-ASIN_Mapping/Fk-Amz mapping data.xlsx', read_only=True)
    ws = wb['Sheet1']
    
    rows = list(ws.iter_rows(values_only=True))
    data = rows[1:]
    
    inserts = []
    seen = set()
    for row in data:
        fsn = str(row[0]).strip() if row[0] else ""
        asin = str(row[1]).strip() if row[1] else ""
        portfolio = str(row[2]).strip() if row[2] else ""
        category = str(row[3]).strip() if row[3] else ""
        subcategory = str(row[4]).strip() if row[4] else ""
        
        if not fsn or not asin:
            continue
            
        key = (asin, fsn)
        if key in seen:
            continue
        seen.add(key)
        
        inserts.append(AsinFsnMapping(
            user=user,
            asin=asin,
            fsn=fsn,
            portfolio=portfolio,
            category=category,
            subcategory=subcategory
        ))
        
    AsinFsnMapping.objects.filter(user=user).delete()
    AsinFsnMapping.objects.bulk_create(inserts, batch_size=2000)
    print(f"Inserted {len(inserts)} mappings.")

if __name__ == '__main__':
    run()
