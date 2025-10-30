# generate_data.py for McCain demo raw layer (full regeneration per updated story and schemas)
# ASCII only
from utils import save_to_parquet
import numpy as np
import pandas as pd
import random
from faker import Faker
from holidays import UnitedStates, Canada

# Set environment variables for Databricks Volumes
import os
os.environ['CATALOG'] = 'brlui'
os.environ['SCHEMA'] = 'brian_lui_mccain'
os.environ['VOLUME'] = 'raw_data'



np.random.seed(42)
random.seed(42)
fake = Faker()

# Global time ranges (tz-naive, ms precision)
DATE_START = pd.Timestamp('2025-05-01').floor('ms')
DATE_END   = pd.Timestamp('2025-10-15 23:59:59').floor('ms')
EVENT_START = pd.Timestamp('2025-09-08').floor('ms')
EVENT_END   = pd.Timestamp('2025-09-15').floor('ms')
EVENT_SPIKE_START = pd.Timestamp('2025-09-09').floor('ms')
EVENT_SPIKE_END   = pd.Timestamp('2025-09-13').floor('ms')

# Domains
PLANTS = [
    ('idaho', 'Idaho Burley Plant'),
    ('wi', 'Wisconsin Plover Plant'),
    ('on', 'Ontario Florenceville Plant')
]
LINES  = ['l1','l2','l3']
VARIETIES = ['russet burbank','russet norkotah','shepody','innovator','ranger']
CORE_SKUS = [
    ('sku-ff-38-skinon','Skin-On Fries 3/8','3/8'),
    ('sku-ff-716','Standard Fries 7/16','7/16'),
    ('sku-wedges-prem','Premium Wedges','wedges'),
    ('sku-hashbrown','Hashbrown Patties','hashbrown'),
    ('sku-ff-crinkle','Crinkle Cut Fries','3/8'),
    ('sku-waffle','Waffle Fries','3/8'),
]
CORE_SKU_IDS = [s[0] for s in CORE_SKUS]

US_HOLIDAYS = UnitedStates(years=[2025])
CA_HOLIDAYS = Canada(years=[2025])

# Helper functions

def _hour_probs():
    # 6..22 hour window (17 hours)
    base = np.array([0.01,0.02,0.04,0.06,0.08,0.10,0.12,0.12,0.10,0.09,0.08,0.07,0.05,0.03,0.02,0.01,0.00])
    base = np.clip(base, 0.0001, None)
    return base / base.sum()


def random_business_hour(size):
    hours = np.random.choice(np.arange(6,23), size=size, p=_hour_probs())
    minutes = np.random.randint(0,60,size)
    return hours, minutes


def weekday_weight(d: pd.Timestamp):
    wd = pd.Timestamp(d).weekday()
    if wd in (1,2,3):
        return 1.2
    elif wd in (5,6):
        return 0.6
    elif wd == 0:
        return 0.9
    else:
        return 1.0


def holiday_modifier(d: pd.Timestamp, plant_id: str):
    if plant_id == 'on':
        return 0.85 if pd.Timestamp(d).date() in CA_HOLIDAYS else 1.0
    else:
        return 0.9 if pd.Timestamp(d).date() in US_HOLIDAYS else 1.0


def generate_suppliers():
    sup_ids = [f"sup-g{i}" for i in range(18,76)]
    weights = np.random.rand(len(sup_ids)) ** 2
    weights = weights / weights.sum()
    return sup_ids, weights

SUPPLIERS, SUP_WEIGHTS = generate_suppliers()


def supplier_region(sup: str) -> str:
    idx = int(sup.split('-g')[-1])
    if idx <= 27:
        return 'se'
    elif idx <= 36:
        return 'sw'
    elif idx <= 50:
        return 'nw'
    elif idx <= 63:
        return 'ne'
    else:
        return 'central'


def variety_prob_by_plant(plant_id: str):
    if plant_id == 'idaho':
        probs = {'russet burbank':0.45,'russet norkotah':0.30,'shepody':0.10,'innovator':0.08,'ranger':0.07}
    elif plant_id == 'wi':
        probs = {'russet burbank':0.35,'russet norkotah':0.25,'shepody':0.20,'innovator':0.10,'ranger':0.10}
    else:
        probs = {'russet burbank':0.40,'russet norkotah':0.20,'shepody':0.15,'innovator':0.15,'ranger':0.10}
    # ensure probs sum to 1
    vals = np.array(list(probs.values()), dtype=float)
    probs = {k: float(v)/float(vals.sum()) for k,v in probs.items()}
    return probs


def choose_line_and_sku(plant_id: str):
    if plant_id == 'idaho':
        line = np.random.choice(LINES, p=np.array([0.35,0.45,0.20]))
    else:
        line = np.random.choice(LINES, p=np.array([0.40,0.30,0.30]))
    if line == 'l2' and plant_id == 'idaho':
        sku = 'sku-ff-38-skinon'
    else:
        sku = np.random.choice(CORE_SKU_IDS)
    return line, sku


def color_bin_probs(dry_solids: float, defects: float, is_event: bool=False):
    # returns probs for 0..4 bins (USDA A..E)
    pA, pB, pC, pD, pE = 0.45, 0.35, 0.15, 0.04, 0.01
    if dry_solids < 19.5:
        pA -= 0.12; pB += 0.05; pC += 0.05; pD += 0.02
    if defects > 1.2:
        pA -= 0.08; pB += 0.03; pC += 0.03; pD += 0.02
    if is_event:
        pA -= 0.05; pB += 0.02; pC += 0.02; pD += 0.01
    ps = np.array([pA,pB,pC,pD,pE], dtype=float)
    ps = np.clip(ps, 0.001, None)
    ps = ps/ps.sum()
    return ps


# -----------------------------
# raw_potato_load_quality
# -----------------------------
def generate_raw_potato_load_quality(row_target: int = 286745) -> pd.DataFrame:
    print(f"Generating raw_potato_load_quality: {row_target:,} rows...")
    dates = pd.date_range(DATE_START, DATE_END, freq='D').to_pydatetime()
    plant_day_counts = {}
    for plant_id, plant_name in PLANTS:
        plant_day_counts[plant_id] = []
        for d in dates:
            ts_d = pd.Timestamp(d)
            base = 180 if plant_id=='idaho' else (140 if plant_id=='wi' else 120)
            m = weekday_weight(ts_d) * holiday_modifier(ts_d, plant_id)
            season_factor = 1.0 + (0.18 if ts_d.month in (7,8,9) else (0.0 if ts_d.month in (5,6) else -0.05))
            count = int(base * m * season_factor)
            plant_day_counts[plant_id].append(count)
    total_est = sum([sum(v) for v in plant_day_counts.values()])
    scale = row_target / total_est
    for plant_id, _ in PLANTS:
        plant_day_counts[plant_id] = [max(25,int(c*scale)) for c in plant_day_counts[plant_id]]

    vprob_map = {pid: variety_prob_by_plant(pid) for pid, _ in PLANTS}
    records = []
    progress_interval = max(1, len(dates)//10)
    load_seq = 0
    for di, d in enumerate(dates):
        ts_d = pd.Timestamp(d)
        if (di + 1) % progress_interval == 0 or di == 0:
            progress = ((di + 1) / len(dates)) * 100
            print(f"  Days processed: {progress:.0f}% ({di + 1:,}/{len(dates):,})")
        for plant_id, plant_name in PLANTS:
            day_count = plant_day_counts[plant_id][di]
            hours, minutes = random_business_hour(day_count)
            seconds = np.random.randint(0,60,day_count)
            hhmmss = hours*10000 + minutes*100 + seconds
            deltas = pd.to_timedelta(hours, unit='h') + pd.to_timedelta(minutes, unit='m') + pd.to_timedelta(seconds, unit='s')
            times = (ts_d + deltas).floor('ms')
            sups = np.random.choice(SUPPLIERS, size=day_count, p=SUP_WEIGHTS)
            sup_idx = np.char.partition(sups, '-g')[:,2].astype(int)
            regions = np.select([sup_idx <= 27, sup_idx <= 36, sup_idx <= 50, sup_idx <= 63], ['se','sw','nw','ne'], default='central')
            vprobs = vprob_map[plant_id]
            varieties = np.random.choice(list(vprobs.keys()), size=day_count, p=np.array(list(vprobs.values())))
            varietylabel = np.where(varieties=='russet burbank','Russet Burbank',
                                    np.where(varieties=='russet norkotah','Russet Norkotah',
                                             np.where(varieties=='shepody','Shepody',
                                                      np.where(varieties=='innovator','Innovator','Ranger'))))
            gross_weight_kg = np.random.lognormal(mean=10.1, sigma=0.15, size=day_count)
            gross_weight_kg = np.clip(gross_weight_kg, 20000, 32000)
            netweightonload = gross_weight_kg - np.random.uniform(1200, 1800, size=day_count)
            netweightonload = np.clip(netweightonload, 18500, 30500)
            base_len = np.random.normal(100, 12, size=day_count)
            base_len += np.where(varieties=='russet burbank', 5, 0)
            base_len += np.where(varieties=='russet norkotah', -3, 0)
            line_sku = np.fromiter((choose_line_and_sku(plant_id) for _ in range(day_count)), dtype=object, count=day_count)
            lines = np.array([ls[0] for ls in line_sku], dtype=object)
            skus = np.array([ls[1] for ls in line_sku], dtype=object)
            seasonal_offset = 0.6 if ts_d.month in (8,9,10) else (0.0 if ts_d.month in (5,6) else 0.3)
            dry_solids = np.random.normal(20.2 + seasonal_offset, 1.0, size=day_count)
            defects = np.clip(np.random.normal(0.95, 0.35, size=day_count), 0.0, 5.0)
            # Event impact: Idaho plant, SE region, Norkotah arrivals concentrated 9/8..9/10
            if (plant_id=='idaho') and (ts_d >= EVENT_START) and (ts_d <= pd.Timestamp('2025-09-10')):
                se_mask = regions=='se'
                rn_mask = varieties=='russet norkotah'
                impacted = se_mask & rn_mask
                if impacted.any():
                    dry_solids[impacted] = np.random.uniform(17.8, 18.9, size=impacted.sum())
                    defects[impacted] = np.clip(defects[impacted] * np.random.uniform(1.12, 1.18, size=impacted.sum()), 0.0, 5.0)
                    idx_imp = np.where(impacted)[0]
                    if idx_imp.size:
                        lines[idx_imp] = 'l2'
                        skus[idx_imp] = 'sku-ff-38-skinon'
            # Over-quality: Burbank routed to L1 during event window (richer dry solids)
            if (plant_id=='idaho') and (ts_d >= EVENT_START) and (ts_d <= EVENT_END):
                rb_mask = (varieties=='russet burbank') & (np.array(lines)=='l1')
                if rb_mask.any():
                    dry_solids[rb_mask] = dry_solids[rb_mask] + np.random.uniform(0.5, 1.5, size=rb_mask.sum())
            usda_color = np.empty(day_count, dtype=object)
            # vectorized-ish: precompute event mask and probabilities for indices, then sample
            event_mask = (plant_id=='idaho') & (times >= EVENT_SPIKE_START) & (times <= EVENT_SPIKE_END)
            idxs = np.arange(day_count)
            for i in idxs:
                ps = color_bin_probs(dry_solids[i], defects[i], is_event=bool(event_mask[i]))
                usda_color[i] = np.random.choice(['a','b','c','d'], p=ps[:4]/ps[:4].sum())
            storage_bin_id = np.array([f"BIN-{random.randint(1,40):03d}" for _ in range(day_count)], dtype=object)
            # ~0.1% nulls in storage_bin_id
            if day_count > 0:
                null_ct = max(1, day_count//1000)
                if null_ct > 0:
                    null_idx = np.random.choice(np.arange(day_count), size=null_ct, replace=False)
                    storage_bin_id[null_idx] = None
            attributecode = np.random.choice(['DRY_SOLIDS','DEFECT_POINTS','AVERAGE_LENGTH','USDA_COLOR'], size=day_count, p=np.array([0.35,0.25,0.25,0.15]))
            sampleattributelabel = np.where(attributecode=='DRY_SOLIDS','Min_Dry_Matter_Pct',
                                     np.where(attributecode=='DEFECT_POINTS','Max_Defect_Points',
                                              np.where(attributecode=='AVERAGE_LENGTH','Average_Length_Grading_Min','Max_USDA_Color_2')))
            attributevalue = np.where(attributecode=='DRY_SOLIDS', dry_solids,
                                       np.where(attributecode=='DEFECT_POINTS', defects,
                                                np.where(attributecode=='AVERAGE_LENGTH', base_len, np.where(usda_color=='a',0,np.where(usda_color=='b',1,np.where(usda_color=='c',2,3))))))
            loadnumbers = np.array([f"TRK-{random.randint(80000,99999)}" for _ in range(day_count)], dtype=object)
            for i in range(day_count):
                load_seq += 1
                load_id = f"ld-{pd.Timestamp(d).strftime('%Y%m%d')}-{int(times[i].strftime('%H%M%S')):06d}-{load_seq:05d}"
                records.append({
                    'load_id': load_id,
                    'plant_id': plant_id,
                    'plant_name': plant_name,
                    'delivery_time': times[i],
                    'supplier_id': sups[i],
                    'field_region': regions[i],
                    'variety': varieties[i],
                    'varietylabel': varietylabel[i],
                    'loadnumber': loadnumbers[i],
                    'gross_weight_kg': float(gross_weight_kg[i]),
                    'netweightonload': float(netweightonload[i]),
                    'avg_length_mm': float(np.clip(base_len[i] + np.random.normal(0,3), 70, 140)),
                    'usda_color': usda_color[i],
                    'defect_points': float(defects[i]),
                    'dry_solids_pct': float(np.clip(dry_solids[i], 16.5, 24.0)),
                    'attributecode': attributecode[i],
                    'sampleattributelabel': sampleattributelabel[i],
                    'attributevalue': float(attributevalue[i]),
                    'intended_line_id': lines[i],
                    'intended_sku_id': skus[i],
                    'storage_bin_id': storage_bin_id[i]
                })
    df = pd.DataFrame.from_records(records)
    # Normalize datetime
    df['delivery_time'] = pd.to_datetime(df['delivery_time'], utc=True, errors='coerce').dt.tz_convert(None).dt.floor('ms')
    # Enforce dtypes
    float_cols = ['gross_weight_kg','netweightonload','avg_length_mm','defect_points','dry_solids_pct','attributevalue']
    for c in float_cols:
        df[c] = df[c].astype(float)
    return df


# -----------------------------
# raw_product_specifications_pss
# -----------------------------
def generate_raw_product_specifications_pss(row_target: int = 1347) -> pd.DataFrame:
    print("Generating raw_product_specifications_pss...")
    vprob_map = {pid: variety_prob_by_plant(pid) for pid, _ in PLANTS}
    records = []
    for plant_id, _ in PLANTS:
        for sku_id, sku_name, cut in CORE_SKUS:
            if sku_id == 'sku-ff-38-skinon':
                min_len, tgt_len, max_len = 85, 105, 130
                max_def = 1.5
                min_ds, max_ds = 20.0, 23.5
                max_color = [60, 35, 10, 5, 0]
                allowed = ['russet burbank','russet norkotah']
            else:
                base_min = 80 if cut in ('3/8','7/16') else (95 if cut=='wedges' else 70)
                base_tgt = base_min + 20
                base_max = base_min + 45
                min_len, tgt_len, max_len = base_min, base_tgt, base_max
                max_def = float(np.round(np.random.uniform(1.2, 1.8),2))
                min_ds = float(np.round(np.random.uniform(19.0, 21.0),1))
                max_ds = float(np.round(min_ds + np.random.uniform(2.5, 4.5),1))
                max_color = list(np.random.randint(0, 80, size=5))
                max_color[0] = max(20, max_color[0])
                max_color[3] = min(10, max_color[3])
                max_color[4] = min(2, max_color[4])
                allowed = list(np.random.choice(VARIETIES, size=np.random.choice([2,3], p=np.array([0.6,0.4])), replace=False))
            records.append({
                'plantid': plant_id,
                'productid': sku_id,
                'average_length_grading_min': float(min_len),
                'average_length_grading_target': float(tgt_len),
                'average_length_grading_max': float(max_len),
                'pct_min_50mm_length': float(np.round(np.random.uniform(85, 98),1)),
                'pct_min_75mm_length': float(np.round(np.random.uniform(70, 90),1)),
                'max_usda_color_0': float(max_color[0]),
                'max_usda_color_1': float(max_color[1]),
                'max_usda_color_2': float(max_color[2]),
                'max_usda_color_3': float(max_color[3]),
                'max_usda_color_4': float(max_color[4]),
                'max_defect_points': float(max_def),
                'min_dry_matter_pct': float(min_ds),
                'max_dry_matter_pct': float(max_ds),
                'approved_potato_varieties': allowed
            })
    i = 1
    while len(records) < row_target:
        plant_id = np.random.choice([p[0] for p in PLANTS], p=np.array([0.45,0.30,0.25]))
        cut = np.random.choice(['3/8','7/16','wedges','hashbrown'], p=np.array([0.45,0.25,0.20,0.10]))
        base_min = 80 if cut in ('3/8','7/16') else (95 if cut=='wedges' else 70)
        min_len = base_min + np.random.randint(-2, 3)
        tgt_len = min_len + np.random.randint(18, 24)
        max_len = tgt_len + np.random.randint(20, 28)
        max_def = float(np.round(np.random.uniform(1.2, 1.9),2))
        min_ds = float(np.round(np.random.uniform(18.5, 21.5),1))
        max_ds = float(np.round(min_ds + np.random.uniform(2.5, 4.0),1))
        max_color = [int(x) for x in np.clip(np.random.normal([60,30,8,3,1], [8,6,4,2,1]), 0, 90)]
        allowed_vars = list(np.random.choice(VARIETIES, size=np.random.choice([2,3], p=np.array([0.65,0.35])), replace=False))
        sku_id = f"sku-auto-{cut.replace('/','')}-{i:04d}"
        records.append({
            'plantid': plant_id,
            'productid': sku_id,
            'average_length_grading_min': float(min_len),
            'average_length_grading_target': float(tgt_len),
            'average_length_grading_max': float(max_len),
            'pct_min_50mm_length': float(np.round(np.random.uniform(85, 98),1)),
            'pct_min_75mm_length': float(np.round(np.random.uniform(70, 90),1)),
            'max_usda_color_0': float(max_color[0]),
            'max_usda_color_1': float(max_color[1]),
            'max_usda_color_2': float(max_color[2]),
            'max_usda_color_3': float(max_color[3]),
            'max_usda_color_4': float(max_color[4]),
            'max_defect_points': float(max_def),
            'min_dry_matter_pct': float(min_ds),
            'max_dry_matter_pct': float(max_ds),
            'approved_potato_varieties': allowed_vars
        })
        i += 1
    df = pd.DataFrame(records)
    return df


# -----------------------------
# raw_line_quality_events_osipi
# -----------------------------
def generate_raw_line_quality_events_osipi(row_target: int = 412385) -> pd.DataFrame:
    print(f"Generating raw_line_quality_events_osipi: target {row_target:,} rows...")
    dates = pd.date_range(DATE_START, DATE_END, freq='D')
    vprob_map = {pid: variety_prob_by_plant(pid) for pid, _ in PLANTS}
    records = []
    events_per_day_base = int(max(600, row_target / max(1,len(dates)) / len(PLANTS)))
    progress_interval = max(1, len(dates)//10)
    for di, day in enumerate(dates):
        d_ts = pd.Timestamp(day)
        if (di + 1) % progress_interval == 0 or di == 0:
            progress = ((di + 1) / len(dates)) * 100
            print(f"  OSIPI days: {progress:.0f}% ({di + 1:,}/{len(dates):,})")
        for plant_id, _ in PLANTS:
            vol = int(events_per_day_base * weekday_weight(d_ts) * holiday_modifier(d_ts, plant_id))
            if vol <= 0:
                continue
            # pick product mix, tilt to skin-on on Idaho
            product_mix = np.array([0.35,0.20,0.15,0.10,0.10,0.10]) if plant_id=='idaho' else np.array([0.25,0.25,0.15,0.15,0.10,0.10])
            product_mix = product_mix / product_mix.sum()
            productid = np.random.choice(CORE_SKU_IDS, size=vol, p=product_mix)
            var_probs = variety_prob_by_plant(plant_id)
            variety = np.random.choice(list(var_probs.keys()), size=vol, p=np.array(list(var_probs.values())))
            # business minutes during day
            minutes = np.random.choice(np.arange(7*60, 22*60), size=vol, replace=True)
            avg_len = np.zeros(vol)
            ds_pct = np.zeros(vol)
            defects = np.zeros(vol)
            color_counts = np.zeros((vol,5), dtype=int)
            for i in range(vol):
                cut = '3/8' if productid[i] in ('sku-ff-38-skinon','sku-ff-crinkle','sku-waffle') else ('7/16' if productid[i]=='sku-ff-716' else ('wedges' if productid[i]=='sku-wedges-prem' else 'hashbrown'))
                base_len = 100 if cut in ('3/8','7/16') else (115 if cut=='wedges' else 95)
                avg_len[i] = float(np.clip(np.random.normal(base_len, 6), 70, 150))
                seasonal_offset = 0.6 if d_ts.month in (8,9,10) else (0.0 if d_ts.month in (5,6) else 0.3)
                ds_pct[i] = float(np.clip(np.random.normal(20.2 + seasonal_offset, 0.8), 16.5, 24.0))
                defects[i] = float(np.clip(np.random.normal(0.95, 0.35), 0.0, 5.0))
                is_event = (plant_id=='idaho') and (EVENT_SPIKE_START <= (d_ts + pd.Timedelta(minutes=int(minutes[i]))) <= EVENT_SPIKE_END) and (productid[i]=='sku-ff-38-skinon')
                if is_event:
                    ds_pct[i] = float(max(16.5, ds_pct[i] - np.random.uniform(0.8, 1.2)))
                    defects[i] = float(np.clip(defects[i] * np.random.uniform(1.15,1.35), 0.0, 5.0))
                ps = color_bin_probs(ds_pct[i], defects[i], is_event=is_event)
                sample_count = int(np.clip(np.random.normal(2500, 900), 200, 10000))
                counts = np.random.multinomial(sample_count, ps)
                color_counts[i,:] = counts
            dt = (d_ts + pd.to_timedelta(minutes, unit='m')).floor('ms')
            for i in range(vol):
                records.append({
                    'datetime': dt[i],
                    'plantid': plant_id,
                    'productid': productid[i],
                    'variety': variety[i].replace(' ', '_'),
                    'average_length_ml': avg_len[i],
                    'usda_color_0': int(color_counts[i,0]),
                    'usda_color_1': int(color_counts[i,1]),
                    'usda_color_2': int(color_counts[i,2]),
                    'usda_color_3': int(color_counts[i,3]),
                    'usda_color_4': int(color_counts[i,4]),
                    'total_defect_points': defects[i],
                    'dry_solids_pct': ds_pct[i]
                })
    df = pd.DataFrame(records)
    # Normalize datetime
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True, errors='coerce').dt.tz_convert(None).dt.floor('ms')
    # Enforce dtypes
    int_cols = ['usda_color_0','usda_color_1','usda_color_2','usda_color_3','usda_color_4']
    for c in int_cols:
        df[c] = df[c].astype(int)
    float_cols = ['average_length_ml','total_defect_points','dry_solids_pct']
    for c in float_cols:
        df[c] = df[c].astype(float)
    return df


# -----------------------------
# raw_oee_equipment_events
# -----------------------------
def generate_raw_oee_equipment_events(row_target: int = 167432) -> pd.DataFrame:
    print(f"Generating raw_oee_equipment_events: {row_target:,} rows...")
    vprob_map = {pid: variety_prob_by_plant(pid) for pid, _ in PLANTS}
    records = []
    days = pd.date_range(DATE_START, DATE_END, freq='D')
    progress_interval = max(1, len(days)//10)
    base_events = max(5, int(row_target / (len(days) * len(PLANTS) * len(LINES))))
    for i, day in enumerate(days):
        if (i + 1) % progress_interval == 0 or i == 0:
            progress = ((i + 1) / len(days)) * 100
            print(f"  OEE days: {progress:.0f}% ({i + 1:,}/{len(days):,})")
        for plant_id, plant_name in PLANTS:
            for line in LINES:
                n_events = int(base_events * weekday_weight(day) * holiday_modifier(day, plant_id))
                if n_events < 3:
                    n_events = 3
                starts = np.sort(np.random.choice(np.arange(7*60, 23*60), size=n_events, replace=False))
                for st in starts:
                    start_ts = (pd.Timestamp(day) + pd.Timedelta(minutes=int(st))).floor('ms')
                    duration = int(np.clip(np.random.normal(45, 20), 10, 180))
                    end_ts = (start_ts + pd.Timedelta(minutes=duration)).floor('ms')
                    is_event_window = (plant_id=='idaho') and (line=='l2') and (start_ts>=EVENT_SPIKE_START) and (start_ts<=EVENT_SPIKE_END)
                    primary = np.random.choice(['QLT-LOW-DRY-SOLIDS','EQP-BLADE-DULL','PROC-OIL-TEMP','SCHED-CHANGEOVER','MAINT-PLANNED'], p=np.array([0.20,0.22,0.18,0.20,0.20]))
                    secondary = np.random.choice(['eqp-cutr-bld-021','oil-temp-alm-118','frzr-def-045','sched-cov-009','maint-lub-002'])
                    cat1 = np.random.choice(['mechanical','operational'], p=np.array([0.55,0.45]))
                    cat2 = np.random.choice(['performance','quality','availability'])
                    product = np.random.choice(CORE_SKU_IDS, p=np.array([0.35,0.20,0.15,0.10,0.10,0.10])) if plant_id=='idaho' else np.random.choice(CORE_SKU_IDS)
                    plant_line = f"{plant_name}-{line}"
                    plant_line_product = f"{plant_line}{product}"
                    workshift = np.random.choice(['A','B','C'], p=np.array([0.45,0.35,0.20]))
                    start_hour = int(pd.Timestamp(start_ts).hour)
                    end_hour = int(pd.Timestamp(end_ts).hour)
                    downtime_dur = int(np.clip(duration * np.random.uniform(0.05, 0.65), 0, duration))
                    qty_packed = float(np.clip(np.random.normal(1800, 650), 500, 5800))
                    num_totes = float(np.clip(np.random.normal(24, 8), 5, 60))
                    if is_event_window:
                        qty_packed *= np.random.uniform(0.92, 0.95)
                        downtime_dur = int(downtime_dur * np.random.uniform(1.1, 1.5))
                        primary = np.random.choice(['QLT-LOW-DRY-SOLIDS','PROC-OIL-TEMP'])
                    records.append({
                        'time_id': pd.Timestamp(day).floor('ms').date(),
                        'plant_id': plant_id,
                        'line_cd': line,
                        'plant_name': plant_name,
                        'plant_line': plant_line,
                        'primary_reason_cd': primary,
                        'secondary_reason_cd': secondary,
                        'downtime_cat_1': cat1,
                        'downtime_cat_2': cat2,
                        'product': product,
                        'plant_line_product': plant_line_product,
                        'workshift': workshift,
                        'start_time': start_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        'start_hour': int(start_hour),
                        'end_time': end_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        'end_hour': int(end_hour),
                        'duration': int(duration),
                        'downtime_duration': int(downtime_dur),
                        'qty_packed': float(qty_packed),
                        'num_totes_on': float(num_totes)
                    })
    df = pd.DataFrame(records)
    # Normalize time_id to date
    df['time_id'] = pd.to_datetime(df['time_id'], errors='coerce').dt.floor('ms').dt.date
    # ints and floats
    int_cols = ['start_hour','end_hour','duration','downtime_duration']
    for c in int_cols:
        df[c] = df[c].astype(int)
    float_cols = ['qty_packed','num_totes_on']
    for c in float_cols:
        df[c] = df[c].astype(float)
    return df


# -----------------------------
# Summary / QA
# -----------------------------
def summarize_signals(loads_df, osipi_df, oee_df):
    print("\nSummary checks:")
    loads_df = loads_df.copy()
    osipi_df = osipi_df.copy()
    # Under-quality share baseline vs event for Idaho L2 intended
    loads_df['date'] = pd.to_datetime(loads_df['delivery_time'], utc=True, errors='coerce').dt.tz_convert(None).dt.floor('ms').dt.date
    mask_idaho_l2 = (loads_df['plant_id']=='idaho') & (loads_df['intended_line_id']=='l2')
    baseline = loads_df[mask_idaho_l2 & (loads_df['date'] < pd.Timestamp('2025-09-08').date())]
    eventwin = loads_df[mask_idaho_l2 & (loads_df['date'] >= pd.Timestamp('2025-09-09').date()) & (loads_df['date'] <= pd.Timestamp('2025-09-13').date())]
    baseline_under = (baseline['dry_solids_pct'] < 20.0).mean() if len(baseline)>0 else np.nan
    event_under = (eventwin['dry_solids_pct'] < 20.0).mean() if len(eventwin)>0 else np.nan
    print(f"- Idaho L2 under-quality (dry solids <20%) baseline: {baseline_under*100:.1f}% vs event window: {event_under*100:.1f}%")
    # OSIPI: color skew and defects during event for idaho skin-on
    osipi_df['date'] = pd.to_datetime(osipi_df['datetime'], utc=True, errors='coerce').dt.tz_convert(None).dt.floor('ms').dt.date
    id_skin = osipi_df[(osipi_df['plantid']=='idaho') & (osipi_df['productid']=='sku-ff-38-skinon')]
    base_mask = id_skin['date'] < pd.Timestamp('2025-09-09').date()
    event_mask = (id_skin['date'] >= pd.Timestamp('2025-09-09').date()) & (id_skin['date'] <= pd.Timestamp('2025-09-13').date())
    base_def = id_skin[base_mask]['total_defect_points'].mean()
    event_def = id_skin[event_mask]['total_defect_points'].mean()
    base_dark_share = (id_skin[base_mask]['usda_color_2'] + id_skin[base_mask]['usda_color_3'] + id_skin[base_mask]['usda_color_4']).sum() / (id_skin[base_mask][['usda_color_0','usda_color_1','usda_color_2','usda_color_3','usda_color_4']].sum().sum() + 1e-9)
    event_dark_share = (id_skin[event_mask]['usda_color_2'] + id_skin[event_mask]['usda_color_3'] + id_skin[event_mask]['usda_color_4']).sum() / (id_skin[event_mask][['usda_color_0','usda_color_1','usda_color_2','usda_color_3','usda_color_4']].sum().sum() + 1e-9)
    print(f"- OSIPI defects baseline: {base_def:.2f} vs event: {event_def:.2f}; dark color share baseline {base_dark_share*100:.1f}% vs event {event_dark_share*100:.1f}%")
    # OEE derate evidence
    oee_id_l2 = oee_df[(oee_df['plant_id']=='idaho') & (oee_df['line_cd']=='l2')]
    oee_id_l2['start_time_dt'] = pd.to_datetime(oee_id_l2['start_time'], errors='coerce')
    oee_id_l2['date'] = oee_id_l2['start_time_dt'].dt.date
    derate_event = oee_id_l2[(oee_id_l2['date'] >= pd.Timestamp('2025-09-09').date()) & (oee_id_l2['date'] <= pd.Timestamp('2025-09-13').date())]['downtime_duration'].mean()
    derate_base = oee_id_l2[oee_id_l2['date'] < pd.Timestamp('2025-09-09').date()]['downtime_duration'].mean()
    print(f"- OEE downtime duration baseline: {derate_base:.1f} min vs event: {derate_event:.1f} min")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    print("Starting data generation...")
    print("-" * 50)
    df_loads = generate_raw_potato_load_quality()
    save_to_parquet(df_loads, "raw_potato_load_quality")

    df_pss = generate_raw_product_specifications_pss()
    save_to_parquet(df_pss, "raw_product_specifications_pss")

    df_osipi = generate_raw_line_quality_events_osipi()
    save_to_parquet(df_osipi, "raw_line_quality_events_osipi")

    df_oee = generate_raw_oee_equipment_events()
    save_to_parquet(df_oee, "raw_oee_equipment_events")

    summarize_signals(df_loads, df_osipi, df_oee)
    print("Data generation complete.")