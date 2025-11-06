# McCain Foods Manufacturing Optimization Demo - RAW Synthetic Data Generator (Regenerated)
# Generates raw tables per demo_story.json with realistic patterns, event windows, and cross-table coherence.
# Contract: exact schemas, naive timestamps floored to ms, coherent cross-table references.

import random
import numpy as np
import pandas as pd
from faker import Faker

from utils import save_to_parquet

# Set environment variables for Databricks Volumes
import os
os.environ['CATALOG'] = 'mccain_ebc_catalog'
os.environ['SCHEMA'] = 'ag_mfg'
os.environ['VOLUME'] = 'raw_data'



# ===============================
# === REPRO & GLOBAL WINDOWS ====
# ===============================
SEED = 42
np.random.seed(SEED)
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# All globals are tz-naive and floored/normalized as needed (no tz conversions elsewhere)
RANGE_START = pd.Timestamp('2025-07-10').floor('ms')
RANGE_END = pd.Timestamp('2025-10-31').floor('ms')
EVENT_START = pd.Timestamp('2025-08-18').floor('ms')
EVENT_END = pd.Timestamp('2025-09-12').floor('ms')
S7_TRANCHE_START = pd.Timestamp('2025-08-17').floor('ms')

DAYS = pd.date_range(RANGE_START.normalize(), RANGE_END.normalize(), freq='D')
DAYS = pd.to_datetime(DAYS, utc=False)

# Plants and lines (referential integrity across tables)
PLANTS = [
    {'plant_id': 1, 'plant_name': 'NA-ID-P01', 'region': 'NA'},
    {'plant_id': 2, 'plant_name': 'NA-ME-P02', 'region': 'NA'},
    {'plant_id': 3, 'plant_name': 'EU-NL-P03', 'region': 'EU'},
    {'plant_id': 4, 'plant_name': 'EU-BE-P04', 'region': 'EU'},
]
LINES_PER_PLANT = {
    'NA-ID-P01': ['L1', 'L2', 'L3', 'L4'],
    'NA-ME-P02': ['L1', 'L2', 'L3'],
    'EU-NL-P03': ['L1', 'L2', 'L3', 'L4', 'L5'],
    'EU-BE-P04': ['L1', 'L2', 'L3', 'L4'],
}
PRODUCTS = [
    {'product_id': 9009, 'product_name': 'SC-9mm'},
    {'product_id': 1313, 'product_name': 'CC-13mm'},
    {'product_id': 2525, 'product_name': 'WG-25mm'},
]
VARIETIES = ['Russet', 'Innovator', 'Shepody', 'Bintje', 'Melody']
SUPPLIERS = ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'S10', 'S11', 'S12']
DM_BANDS = ['<=21%', '21-22%', '22-23%', '>=23%']

# Fast lookup constants (deterministic)
PRODUCT_NAME_TO_ID = {p['product_name']: str(p['product_id']) for p in PRODUCTS}
PRODUCT_NAMES = [p['product_name'] for p in PRODUCTS]
AVG_LEN_TARGET = {'SC-9mm': 60, 'CC-13mm': 70, 'WG-25mm': 80}
SUPPLIERS_NO_S7 = [s for s in SUPPLIERS if s != 'S7']

# Precomputed constants for performance (deterministic; no RNG usage)
HOURS_ARR = np.arange(24)
# OEE hour probabilities (weekday/weekend) - must sum to 1
HOUR_PROBS_WEEKDAY_OEE = np.array([
    0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.04, 0.06, 0.08, 0.09, 0.10, 0.10,
    0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.03, 0.02, 0.02, 0.01, 0.01,
])
HOUR_PROBS_WEEKEND_OEE = np.array([
    0.01, 0.01, 0.01, 0.01, 0.015, 0.02, 0.03, 0.04, 0.06, 0.08, 0.09, 0.09,
    0.08, 0.07, 0.06, 0.05, 0.05, 0.04, 0.035, 0.03, 0.025, 0.02, 0.015, 0.01,
])
HOUR_PROBS_WEEKDAY_OEE = HOUR_PROBS_WEEKDAY_OEE / HOUR_PROBS_WEEKDAY_OEE.sum()
HOUR_PROBS_WEEKEND_OEE = HOUR_PROBS_WEEKEND_OEE / HOUR_PROBS_WEEKEND_OEE.sum()

# OSIPI hour probabilities (weekday/weekend)
HOUR_PROBS_WEEKDAY_OSIPI = np.array([
    0.01, 0.01, 0.01, 0.01, 0.015, 0.02, 0.03, 0.05, 0.08, 0.09, 0.10, 0.10,
    0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.03, 0.02, 0.02, 0.015, 0.01,
])
HOUR_PROBS_WEEKEND_OSIPI = np.array([
    0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.04, 0.06, 0.08, 0.09, 0.09,
    0.08, 0.07, 0.06, 0.05, 0.05, 0.045, 0.04, 0.035, 0.03, 0.02, 0.015, 0.01,
])
HOUR_PROBS_WEEKDAY_OSIPI = HOUR_PROBS_WEEKDAY_OSIPI / HOUR_PROBS_WEEKDAY_OSIPI.sum()
HOUR_PROBS_WEEKEND_OSIPI = HOUR_PROBS_WEEKEND_OSIPI / HOUR_PROBS_WEEKEND_OSIPI.sum()

# Precomputed product probabilities and names
PROD_PROBS = np.array([0.45, 0.35, 0.20])
PROD_PROBS = PROD_PROBS / PROD_PROBS.sum()

# Helper to choose with normalized probabilities
def _choose(values, probs, size):
    p = np.array(probs, dtype=float)
    p = p / p.sum()
    return np.random.choice(values, size=size, p=p)

# ===============================
# === 1) PRODUCT SPECIFICATIONS PSS (RAW)
# ===============================
# Schema columns:
# plant_id (int), plant_name (string), plant_line (string), product_id (bigint), product_name (string),
# average_length_grading_mm_min (int), average_length_grading_mm_target (int), average_length_grading_mm_max (int),
# pct_min_50mm_length (double), pct_min_75mm_length (double),
# max_usda_color_0_pct (double), max_usda_color_1_pct (double), max_usda_color_2_pct (double), max_usda_color_3_pct (double), max_usda_color_4_pct (double),
# max_defect_points (int), min_dry_matter_pct (double), max_dry_matter_pct (double),
# approved_potato_varieties (array<string>)

def generate_raw_product_specifications_pss() -> pd.DataFrame:
    print('Generating raw_product_specifications_pss...')
    rows = []
    # Targets by SKU
    sku_targets = {
        'SC-9mm': {'avg_len': (55, 60, 65), 'dm_target': 21.8, 'tol': 0.3},
        'CC-13mm': {'avg_len': (65, 70, 80), 'dm_target': 22.5, 'tol': 0.3},
        'WG-25mm': {'avg_len': (75, 80, 85), 'dm_target': 21.9, 'tol': 0.3},
    }
    # USDA color percentage caps by SKU (0..100)
    color_caps_pct = {
        'SC-9mm': (25.0, 70.0, 25.0, 10.0, 2.0),
        'CC-13mm': (20.0, 65.0, 30.0, 10.0, 2.0),
        'WG-25mm': (28.0, 68.0, 25.0, 9.0, 2.0),
    }
    defect_caps = {'SC-9mm': 2400, 'CC-13mm': 2600, 'WG-25mm': 2500}

    for plant in PLANTS:
        plant_name = plant['plant_name']
        for line in LINES_PER_PLANT[plant_name]:
            for prod in PRODUCTS:
                pname = prod['product_name']
                pid = int(prod['product_id'])
                avg_min, avg_tgt, avg_max = sku_targets[pname]['avg_len']
                dm_tgt = sku_targets[pname]['dm_target']
                tol = sku_targets[pname]['tol']
                min_dm = round(dm_tgt - tol, 2)
                max_dm = round(dm_tgt + tol, 2)
                c0, c1, c2, c3, c4 = color_caps_pct[pname]
                rows.append(
                    {
                        'plant_id': int(plant['plant_id']),
                        'plant_name': plant_name,
                        'plant_line': line,
                        'product_id': np.int64(pid),
                        'product_name': pname,
                        'average_length_grading_mm_min': int(avg_min),
                        'average_length_grading_mm_target': int(avg_tgt),
                        'average_length_grading_mm_max': int(avg_max),
                        'pct_min_50mm_length': float(np.random.uniform(0.80, 0.95)),
                        'pct_min_75mm_length': float(np.random.uniform(0.40, 0.70)),
                        'max_usda_color_0_pct': float(c0),
                        'max_usda_color_1_pct': float(c1),
                        'max_usda_color_2_pct': float(c2),
                        'max_usda_color_3_pct': float(c3),
                        'max_usda_color_4_pct': float(c4),
                        'max_defect_points': int(defect_caps[pname]),
                        'min_dry_matter_pct': float(min_dm),
                        'max_dry_matter_pct': float(max_dm),
                        'approved_potato_varieties': list(np.random.choice(VARIETIES, size=3, replace=False)),
                    }
                )
    df = pd.DataFrame(rows)
    # Very small null rate in pct_min_75mm_length (non-critical) ~0.2%
    mask_null = np.random.rand(len(df)) < 0.002
    df.loc[mask_null, 'pct_min_75mm_length'] = np.nan
    return df

# ===============================
# === 2) LINE EQUIPMENT INVENTORY (RAW)
# ===============================
# Schema columns:
# plant_id (int), plant_name (string), plant_line (string), equipment_id (string), equipment_type (string),
# equipment_description (string), equipment_rated_throughput_tph (double), commission_date (date)

def generate_raw_line_equipment() -> pd.DataFrame:
    print('Generating raw_line_equipment...')
    equipment_catalog = [
        ('CUT-2000', 'cutter', 'High-capacity potato cutter, model CUT-2000'),
        ('FRY-XL', 'fryer', 'Continuous fryer XL with precise temp control'),
        ('WASH-900', 'washer', 'Drum washer for pre-process cleaning'),
        ('FREEZE-6', 'freezer', 'Cryo freezer tunnel model 6'),
        ('SORT-AI', 'sorter', 'Optical sorter with AI defect removal'),
    ]
    rows = []
    for plant in PLANTS:
        plant_name = plant['plant_name']
        for line in LINES_PER_PLANT[plant_name]:
            # Assign typical equipment set per line
            for eq_id, eq_type, eq_desc in equipment_catalog:
                rated = {
                    'cutter': np.random.uniform(6.5, 9.0),
                    'fryer': np.random.uniform(4.5, 6.5),
                    'washer': np.random.uniform(7.0, 10.0),
                    'freezer': np.random.uniform(5.0, 7.5),
                    'sorter': np.random.uniform(6.0, 8.5),
                }[eq_type]
                # Commission dates vary 2015..2024
                year = np.random.randint(2015, 2025)
                month = np.random.randint(1, 13)
                day = np.random.randint(1, 28)
                commission_date = pd.Timestamp(year=year, month=month, day=day).normalize().floor('ms')
                rows.append(
                    {
                        'plant_id': int(plant['plant_id']),
                        'plant_name': plant_name,
                        'plant_line': line,
                        'equipment_id': eq_id,
                        'equipment_type': eq_type,
                        'equipment_description': eq_desc,
                        'equipment_rated_throughput_tph': float(round(rated, 2)),
                        'commission_date': commission_date.date(),
                    }
                )
    df = pd.DataFrame(rows)
    # Small null rate in equipment_description ~0.1%
    mask_null = np.random.rand(len(df)) < 0.001
    df.loc[mask_null, 'equipment_description'] = None
    return df

# ===============================
# === 3) RAW POTATO LOAD QUALITY (RAW)
# ===============================
# Schema columns:
# plant_id (int), plant_name (string), VarietyLabel (string), load_number (string), effective_actual_weight (int),
# average_length_grading_mm (int), usda_color_0_pct (double), usda_color_1_pct (double), usda_color_2_pct (double),
# usda_color_3_pct (double), usda_color_4_pct (double), total_defect_points (int), dry_matter_pct (double)

def generate_raw_potato_load_quality() -> pd.DataFrame:
    print('Generating raw_potato_load_quality...')
    rows = []
    for plant in PLANTS:
        plant_name = plant['plant_name']
        plant_id = plant['plant_id']
        for d in DAYS:
            day = pd.Timestamp(d).normalize().floor('ms')
            weekday = day.weekday()
            base_loads = np.random.poisson(lam=18 if weekday < 5 else 12)
            base_loads = max(base_loads, 6)
            # Event impacts: EU-NL-P03 receives higher S7 share after tranche start
            s7_share = 0.12
            if (plant_name == 'EU-NL-P03') and (day >= S7_TRANCHE_START):
                s7_share = 0.40
            for i in range(base_loads):
                is_s7 = np.random.rand() < s7_share
                supplier_code = 'S7' if is_s7 else np.random.choice(SUPPLIERS_NO_S7)
                variety = np.random.choice(VARIETIES)
                seq = i + 1
                load_number = f"{plant_name}-{day.strftime('%Y%m%d')}-{seq:03d}-{supplier_code}"
                # Weight (kg) 12k..28k log-normal
                eff_wt = int(np.clip(np.random.lognormal(mean=np.log(18_000), sigma=0.35), 12_000, 28_000))
                # Average length grading within 50..85
                avg_len = int(np.clip(np.random.normal(70, 6), 50, 85))
                # USDA color % shares
                if is_s7 and (plant_name == 'EU-NL-P03') and (day >= S7_TRANCHE_START):
                    dm = float(np.random.uniform(23.2, 24.2))
                    color1 = np.random.uniform(57, 72)
                    color0 = np.random.uniform(6, 14)
                    color2 = np.random.uniform(8, 18)
                else:
                    dm = float(np.random.lognormal(mean=np.log(21.5), sigma=0.08))
                    dm = float(np.clip(dm, 20.0, 24.5))
                    color1 = np.random.uniform(50, 65)
                    color0 = np.random.uniform(5, 15)
                    color2 = np.random.uniform(12, 22)
                color3 = np.random.uniform(0.5, 5.0)
                color4 = np.random.uniform(0.0, 1.2)
                total_color = color0 + color1 + color2 + color3 + color4
                color0 = color0 * 100.0 / total_color
                color1 = color1 * 100.0 / total_color
                color2 = color2 * 100.0 / total_color
                color3 = color3 * 100.0 / total_color
                color4 = color4 * 100.0 / total_color
                base_def = np.random.randint(800, 1600)
                if (plant_name == 'NA-ID-P01') and (EVENT_START <= day <= EVENT_END):
                    base_def += np.random.randint(600, 1400)
                rows.append(
                    {
                        'plant_id': int(plant_id),
                        'plant_name': plant_name,
                        'VarietyLabel': variety,
                        'load_number': load_number,
                        'effective_actual_weight': int(eff_wt),
                        'average_length_grading_mm': int(avg_len),
                        'usda_color_0_pct': float(round(color0, 2)),
                        'usda_color_1_pct': float(round(color1, 2)),
                        'usda_color_2_pct': float(round(color2, 2)),
                        'usda_color_3_pct': float(round(color3, 2)),
                        'usda_color_4_pct': float(round(color4, 2)),
                        'total_defect_points': int(base_def),
                        'dry_matter_pct': float(round(dm, 2)),
                    }
                )
    df = pd.DataFrame(rows)
    # Small fraction null in VarietyLabel (~0.3%)
    mask_null = np.random.rand(len(df)) < 0.003
    df.loc[mask_null, 'VarietyLabel'] = None
    return df

# ===============================
# === 4) OEE PRODUCTION RUNS AND DOWNTIME EVENTS (RAW)
# ===============================
# Schema columns:
# datetime (timestamp), plant_id (int), plant_name (string), plant_line (string),
# downtime_cat_1 (string), downtime_cat_2 (string), product_name (string),
# start_time (timestamp), end_time (string), duration (bigint), downtime_duration (bigint),
# qty_packed (double), num_totes_on (double)

def generate_raw_oee_production_runs_and_downtime_events() -> pd.DataFrame:
    print('Generating raw_oee_production_runs_and_downtime_events...')
    rows = []
    for plant in PLANTS:
        plant_name = plant['plant_name']
        plant_id = plant['plant_id']
        lines = LINES_PER_PLANT[plant_name]
        for line in lines:
            for d in DAYS:
                day = pd.Timestamp(d).normalize()
                weekday = day.weekday()
                segments = np.random.poisson(lam=9 if weekday < 5 else 7)
                segments = max(segments, 4)
                prod_probs = PROD_PROBS
                for s in range(segments):
                    hour_probs = HOUR_PROBS_WEEKDAY_OEE if weekday < 5 else HOUR_PROBS_WEEKEND_OEE
                    start_hour = int(np.random.choice(HOURS_ARR, p=hour_probs))
                    start_min = int(np.random.randint(0, 60))
                    start_time = (day + pd.Timedelta(hours=start_hour, minutes=start_min)).floor('ms')
                    dur_sec = int(np.random.lognormal(mean=np.log(3600), sigma=0.6))
                    dur_sec = int(np.clip(dur_sec, 600, 18_000))
                    end_time_ts = (start_time + pd.Timedelta(seconds=dur_sec)).floor('ms')
                    r = np.random.rand()
                    if r < 0.15:
                        end_time_str = end_time_ts.strftime('%m/%d/%Y %H:%M:%S')
                    elif r < 0.20:
                        end_time_str = end_time_ts.strftime('%Y-%m-%d %H:%M')
                    else:
                        end_time_str = end_time_ts.strftime('%Y-%m-%d %H:%M:%S')
                    prod_idx = int(np.random.choice(len(PRODUCTS), p=prod_probs))
                    product_name = PRODUCTS[prod_idx]['product_name']
                    is_downtime = np.random.rand() < (0.22 if weekday < 5 else 0.27)
                    dt_cat_1 = 'Maintenance' if is_downtime and (np.random.rand() < 0.5) else ('Cleaning' if is_downtime else 'Run')
                    dt_cat_2 = 'Cutter' if is_downtime else 'N/A'
                    if (plant_name == 'NA-ID-P01') and (line == 'L3'):
                        if pd.Timestamp('2025-08-22') <= day <= pd.Timestamp('2025-09-05') and is_downtime:
                            dt_cat_2 = np.random.choice(['Cutter DOW-PI-4521', 'Cutter DOW-PI-4574', 'Fryer'])
                    dt_duration = int(np.random.randint(0, int(dur_sec * (0.6 if is_downtime else 0.15))))
                    if (plant_name == 'NA-ID-P01') and (line == 'L3') and (EVENT_START <= day <= EVENT_END):
                        if is_downtime:
                            dt_duration = int(np.clip(dt_duration + np.random.randint(600, 3600), 300, dur_sec))
                    base_rate = np.random.uniform(3.5, 7.0)
                    qty_packed = float(max(0.0, base_rate * (dur_sec - dt_duration)))
                    totes_on = float(np.random.uniform(1.0, 10.0))

                    rows.append(
                        {
                            'datetime': start_time,
                            'plant_id': int(plant_id),
                            'plant_name': plant_name,
                            'plant_line': line,
                            'downtime_cat_1': dt_cat_1,
                            'downtime_cat_2': dt_cat_2,
                            'product_name': product_name,
                            'start_time': start_time,
                            'end_time': end_time_str,
                            'duration': np.int64(dur_sec),
                            'downtime_duration': np.int64(dt_duration),
                            'qty_packed': float(round(qty_packed, 2)),
                            'num_totes_on': float(round(totes_on, 2)),
                        }
                    )
    df = pd.DataFrame(rows)
    # Normalize datetime columns to ms
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce').dt.floor('ms')
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce').dt.floor('ms')
    # Tiny null fraction in num_totes_on (~0.2%)
    mask_null = np.random.rand(len(df)) < 0.002
    df.loc[mask_null, 'num_totes_on'] = np.nan
    return df

# ===============================
# === 5) OSI PI LINE QUALITY EVENTS (RAW)
# ===============================
# Schema columns:
# datetime (timestamp), plant_id (int), plant_name (string), product_id (string), product_name (string), variety (string),
# avg_length_mm (int), usda_color_0 (bigint), usda_color_1 (bigint), usda_color_2 (bigint), usda_color_3 (bigint),
# usda_color_4 (bigint), total_defect_points (bigint), dry_solids_pct (double)

def generate_raw_line_quality_events_osipi() -> pd.DataFrame:
    print('Generating raw_line_quality_events_osipi...')
    rows = []
    progress_total = len(PLANTS) * len(DAYS)
    progress_interval = max(1, progress_total // 10)
    step = 0
    for plant in PLANTS:
        plant_id = plant['plant_id']
        plant_name = plant['plant_name']
        for d in DAYS:
            if (step % progress_interval == 0) or (step == 0):
                progress = (step + 1) / progress_total * 100
                print(f"  Step: {progress:.0f}% ({step + 1:,}/{progress_total:,})")
            step += 1
            day = pd.Timestamp(d).normalize()
            weekday = day.weekday()
            hours_count = np.random.poisson(lam=20 if weekday < 5 else 12)
            hours_count = int(np.clip(hours_count, 8, 22))
            hour_probs = HOUR_PROBS_WEEKDAY_OSIPI if weekday < 5 else HOUR_PROBS_WEEKEND_OSIPI
            hours = np.random.choice(HOURS_ARR, size=hours_count, replace=False, p=hour_probs)
            prods = np.random.choice(PRODUCT_NAMES, size=hours_count, p=PROD_PROBS)
            for hh, pname in zip(hours, prods):
                avg_length_mm = int(np.clip(np.random.normal(AVG_LEN_TARGET[pname], 4), 50, 85))
                base_total = np.random.randint(3500, 6500)
                if pname == 'CC-13mm' and plant_name == 'NA-ID-P01' and (EVENT_START <= day <= EVENT_END):
                    shares = np.array([0.06, 0.50, 0.30, 0.11, 0.03])
                    defect_points = np.random.randint(2600, 3800)
                    dry_solids = float(np.random.uniform(20.6, 21.0))
                elif pname == 'SC-9mm' and plant_name == 'EU-NL-P03' and (EVENT_START <= day <= EVENT_END):
                    shares = np.array([0.08, 0.62, 0.23, 0.06, 0.01])
                    defect_points = np.random.randint(1200, 2000)
                    dry_solids = float(np.random.uniform(23.4, 23.9))
                else:
                    shares = np.array([0.07, 0.65, 0.20, 0.07, 0.01])
                    defect_points = np.random.randint(1200, 2400)
                    base_dm = np.random.normal(22.0 if pname != 'CC-13mm' else 22.6, 0.35)
                    dry_solids = float(np.clip(base_dm, 21.0, 24.0))
                shares = shares / shares.sum()
                counts = np.random.multinomial(n=base_total, pvals=shares)
                c0, c1, c2, c3, c4 = counts
                dt = (day + pd.Timedelta(hours=int(hh))).floor('ms')
                rows.append(
                    {
                        'datetime': dt,
                        'plant_id': int(plant_id),
                        'plant_name': plant_name,
                        'product_id': PRODUCT_NAME_TO_ID[pname],
                        'product_name': pname,
                        'variety': np.random.choice(VARIETIES),
                        'avg_length_mm': int(avg_length_mm),
                        'usda_color_0': np.int64(c0),
                        'usda_color_1': np.int64(c1),
                        'usda_color_2': np.int64(c2),
                        'usda_color_3': np.int64(c3),
                        'usda_color_4': np.int64(c4),
                        'total_defect_points': np.int64(defect_points),
                        'dry_solids_pct': float(round(dry_solids, 2)),
                    }
                )
    df = pd.DataFrame(rows)
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce').dt.floor('ms')
    # Tiny null rate in variety (~0.2%)
    mask_null = np.random.rand(len(df)) < 0.002
    df.loc[mask_null, 'variety'] = None
    return df

# ===============================
# === 6) POTATO VARIETY COSTS (RAW)
# ===============================
# Schema columns:
# effective_date (date), region (string), supplier_id (string), variety (string), dm_band (string), price_usd_per_ton (double), contract_id (string)

def generate_raw_potato_variety_costs() -> pd.DataFrame:
    print('Generating raw_potato_variety_costs...')
    weeks = pd.date_range(RANGE_START.normalize(), RANGE_END.normalize(), freq='7D')
    regions = ['NA', 'EU']
    rows = []
    for eff in weeks:
        eff_date = pd.Timestamp(eff).normalize().floor('ms').date()
        for region in regions:
            supplier_probs = np.array([0.06, 0.06, 0.18, 0.06, 0.06, 0.06, 0.16, 0.05, 0.06, 0.06, 0.07, 0.08])
            supplier_probs = supplier_probs / supplier_probs.sum()
            suppliers = _choose(SUPPLIERS, supplier_probs, size=len(SUPPLIERS))
            for supplier_id in suppliers:
                for variety in VARIETIES:
                    for dm_band in DM_BANDS:
                        base = np.clip(np.random.lognormal(mean=np.log(220), sigma=0.25), 180, 360)
                        if dm_band == '>=23%':
                            base *= np.random.uniform(1.08, 1.18)
                        if region == 'EU' and supplier_id == 'S7' and pd.Timestamp(eff) >= S7_TRANCHE_START:
                            base *= np.random.uniform(1.05, 1.12)
                        price = float(round(base, 2))
                        if supplier_id == 'S7' and pd.Timestamp(eff) >= S7_TRANCHE_START:
                            contract_id = 'S7-2025Q3'
                        else:
                            contract_id = f"{supplier_id}-2025Q3"
                        rows.append(
                            {
                                'effective_date': eff_date,
                                'region': region,
                                'supplier_id': supplier_id,
                                'variety': variety,
                                'dm_band': dm_band,
                                'price_usd_per_ton': price,
                                'contract_id': contract_id,
                            }
                        )
    df = pd.DataFrame(rows)
    return df

# ===============================
# === 7) PRODUCT REVENUE BY LINE (RAW)
# ===============================
# Schema columns:
# date (date), plant_id (int), plant_name (string), plant_line (string), product_id (string), product_name (string),
# units_packed_kg (double), net_sales_usd (double), channel (string), tier (string)

def generate_raw_product_revenue_by_line() -> pd.DataFrame:
    print('Generating raw_product_revenue_by_line...')
    rows = []
    channels = ['Retail', 'Foodservice', 'PrivateLabel']
    tiers = ['Standard', 'Premium']
    for plant in PLANTS:
        plant_id = plant['plant_id']
        plant_name = plant['plant_name']
        region = plant['region']
        for line in LINES_PER_PLANT[plant_name]:
            for d in DAYS:
                date = pd.Timestamp(d).normalize().floor('ms').date()
                weekday = pd.Timestamp(d).weekday()
                for prod in PRODUCTS:
                    pname = prod['product_name']
                    pid = str(prod['product_id'])
                    base_units = np.clip(np.random.lognormal(mean=np.log(24_000), sigma=0.35), 12_000, 48_000)
                    if weekday >= 5:
                        base_units *= np.random.uniform(0.80, 0.87)
                    if plant_name == 'NA-ID-P01' and line == 'L3' and pname == 'CC-13mm' and (EVENT_START <= pd.Timestamp(d) <= EVENT_END):
                        base_units *= np.random.uniform(0.72, 0.85)
                    ch_probs = np.array([0.50, 0.35, 0.15]) if region == 'EU' else np.array([0.35, 0.50, 0.15])
                    ch_probs = ch_probs / ch_probs.sum()
                    channel = np.random.choice(channels, p=ch_probs)
                    tier = np.random.choice(tiers, p=np.array([0.75, 0.25]))
                    price_per_kg = np.random.uniform(0.95, 1.35) if tier == 'Standard' else np.random.uniform(1.30, 1.80)
                    net_sales = float(round(base_units * price_per_kg, 2))
                    rows.append(
                        {
                            'date': date,
                            'plant_id': int(plant_id),
                            'plant_name': plant_name,
                            'plant_line': line,
                            'product_id': pid,
                            'product_name': pname,
                            'units_packed_kg': float(round(base_units, 2)),
                            'net_sales_usd': net_sales,
                            'channel': channel,
                            'tier': tier,
                        }
                    )
    df = pd.DataFrame(rows)
    # Tiny null rate in tier (~0.2%)
    mask_null = np.random.rand(len(df)) < 0.002
    df.loc[mask_null, 'tier'] = None
    return df

# ===============================
# === 8) CURRENT PRODUCT DEMAND (RAW)
# ===============================
# Schema columns:
# date (date), region (string), channel (string), product_id (string), product_name (string),
# orders_kg (double), forecast_kg (double), promo_flag (boolean)

def generate_raw_current_product_demand() -> pd.DataFrame:
    print('Generating raw_current_product_demand...')
    rows = []
    regions = ['NA', 'EU']
    channels = ['Retail', 'Foodservice', 'PrivateLabel']
    promo_start = pd.Timestamp('2025-08-01').normalize()
    promo_end = pd.Timestamp('2025-08-31').normalize()
    for d in DAYS:
        day_ts = pd.Timestamp(d).normalize()
        date = day_ts.floor('ms').date()
        weekday = day_ts.weekday()
        dow_mul = 1.15 if weekday == 0 else (0.90 if weekday == 6 else 1.0)
        is_promo_day = (pd.Timestamp('2025-08-01').normalize() <= day_ts <= pd.Timestamp('2025-08-31').normalize())
        for region in regions:
            for channel in channels:
                for prod in PRODUCTS:
                    pname = prod['product_name']
                    pid = str(prod['product_id'])
                    base_orders = np.clip(np.random.lognormal(mean=np.log(20_000), sigma=0.55), 5_000, 60_000)
                    if region == 'EU' and channel == 'Retail' and (promo_start <= pd.Timestamp(d) <= promo_end):
                        promo_flag = True
                        base_orders *= np.random.uniform(1.15, 1.30)
                    else:
                        promo_flag = False
                    base_orders *= dow_mul
                    if region == 'NA' and pname == 'SC-9mm':
                        base_orders *= np.random.uniform(1.05, 1.20)
                    if region == 'EU' and pname == 'CC-13mm':
                        base_orders *= np.random.uniform(1.08, 1.25)
                    orders = float(round(base_orders, 2))
                    if region == 'EU' and (promo_start <= pd.Timestamp(d) <= promo_end):
                        forecast = float(round(orders * np.random.uniform(1.03, 1.08), 2))
                    else:
                        forecast = float(round(orders * np.random.uniform(0.98, 1.05), 2))
                    rows.append(
                        {
                            'date': date,
                            'region': region,
                            'channel': channel,
                            'product_id': pid,
                            'product_name': pname,
                            'orders_kg': orders,
                            'forecast_kg': forecast,
                            'promo_flag': bool(promo_flag),
                        }
                    )
    df = pd.DataFrame(rows)
    return df

# ===============================
# === SUMMARY / QA ASSERTIONS ===
# ===============================

def print_story_signals_summary(osipi: pd.DataFrame, loads: pd.DataFrame, oee: pd.DataFrame):
    print('\nQuick QA summary:')
    # OSI PI EU-NL-P03 SC-9mm over-quality
    osipi['datetime'] = pd.to_datetime(osipi['datetime'], errors='coerce').dt.tz_localize(None).dt.floor('ms')
    mask_over = (
        (osipi['plant_name'] == 'EU-NL-P03')
        & (osipi['product_name'] == 'SC-9mm')
        & (osipi['datetime'] >= EVENT_START)
        & (osipi['datetime'] <= EVENT_END)
    )
    over_dm = osipi.loc[mask_over, 'dry_solids_pct']
    print(f"  EU-NL-P03 SC-9mm event DM avg: {over_dm.mean():.2f}% (n={len(over_dm)})")

    # OSI PI NA-ID-P01 CC-13mm under-quality & defects
    mask_under = (
        (osipi['plant_name'] == 'NA-ID-P01')
        & (osipi['product_name'] == 'CC-13mm')
        & (osipi['datetime'] >= EVENT_START)
        & (osipi['datetime'] <= EVENT_END)
    )
    under_dm = osipi.loc[mask_under, 'dry_solids_pct']
    under_def = osipi.loc[mask_under, 'total_defect_points']
    print(f"  NA-ID-P01 CC-13mm event DM avg: {under_dm.mean():.2f}% defects avg: {under_def.mean():.1f} (n={len(under_dm)})")

    # Loads: S7 high-DM at EU-NL-P03 post 2025-08-17
    loads_dates = loads['load_number'].str.split('-').str[1]
    loads_dates = pd.to_datetime(loads_dates, format='%Y%m%d', errors='coerce')
    mask_s7 = (loads['plant_name'] == 'EU-NL-P03') & (loads['load_number'].str.contains('S7')) & (loads_dates >= S7_TRANCHE_START)
    s7_dm = loads.loc[mask_s7, 'dry_matter_pct']
    print(f"  EU-NL-P03 S7 loads DM avg post-tranche: {s7_dm.mean():.2f}% (n={len(s7_dm)})")

    # OEE: downtime spike NA-ID-P01 L3 during event
    oee['datetime'] = pd.to_datetime(oee['datetime'], errors='coerce').dt.tz_localize(None).dt.floor('ms')
    mask_oee = (
        (oee['plant_name'] == 'NA-ID-P01')
        & (oee['plant_line'] == 'L3')
        & (oee['datetime'] >= EVENT_START)
        & (oee['datetime'] <= EVENT_END)
        & (oee['downtime_cat_1'] != 'Run')
    )
    print(f"  OEE downtime events NA-ID-P01 L3 during event: {mask_oee.sum():,}")

# ===============================
# === MAIN
# ===============================
if __name__ == '__main__':
    print('Starting McCain Foods RAW data generation (Regenerated)...')
    print('-' * 60)

    # 1) Product specs (reference)
    raw_pss = generate_raw_product_specifications_pss()
    print(f"raw_product_specifications_pss rows: {len(raw_pss):,}")
    save_to_parquet(raw_pss, 'raw_product_specifications_pss', num_files=2)

    # 2) Line equipment inventory
    raw_equipment = generate_raw_line_equipment()
    print(f"raw_line_equipment rows: {len(raw_equipment):,}")
    save_to_parquet(raw_equipment, 'raw_line_equipment', num_files=1)

    # 3) Potato load quality
    raw_loads = generate_raw_potato_load_quality()
    print(f"raw_potato_load_quality rows: {len(raw_loads):,}")
    save_to_parquet(raw_loads, 'raw_potato_load_quality', num_files=6)

    # 4) OEE production runs and downtime events
    raw_oee = generate_raw_oee_production_runs_and_downtime_events()
    print(f"raw_oee_production_runs_and_downtime_events rows: {len(raw_oee):,}")
    save_to_parquet(raw_oee, 'raw_oee_production_runs_and_downtime_events', num_files=6)

    # 5) OSI PI line quality events
    raw_osipi = generate_raw_line_quality_events_osipi()
    print(f"raw_line_quality_events_osipi rows: {len(raw_osipi):,}")
    save_to_parquet(raw_osipi, 'raw_line_quality_events_osipi', num_files=8)

    # 6) Variety costs
    raw_variety_costs = generate_raw_potato_variety_costs()
    print(f"raw_potato_variety_costs rows: {len(raw_variety_costs):,}")
    save_to_parquet(raw_variety_costs, 'raw_potato_variety_costs', num_files=2)

    # 7) Revenue by line
    raw_revenue = generate_raw_product_revenue_by_line()
    print(f"raw_product_revenue_by_line rows: {len(raw_revenue):,}")
    save_to_parquet(raw_revenue, 'raw_product_revenue_by_line', num_files=4)

    # 8) Current demand
    raw_demand = generate_raw_current_product_demand()
    print(f"raw_current_product_demand rows: {len(raw_demand):,}")
    save_to_parquet(raw_demand, 'raw_current_product_demand', num_files=3)

    # QA summary
    print_story_signals_summary(raw_osipi, raw_loads, raw_oee)

    print('\n' + '=' * 60)
    print('GENERATION COMPLETE - All timestamps are naive, floored to ms.')
    print('=' * 60)
