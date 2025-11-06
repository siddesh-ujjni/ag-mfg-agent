# McCain Foods Manufacturing Optimization Demo - RAW Synthetic Data Generator
# Generates five raw tables per demo_story.json with realistic patterns and event windows.
# Contract: exact schemas, naive timestamps floored to ms, coherent cross-table references.

import random
import string

import numpy as np
import pandas as pd
from faker import Faker

from utils import save_to_parquet

# Set environment variables for Databricks Volumes
import os
os.environ['CATALOG'] = 'demo_generator'
os.environ['SCHEMA'] = 'brian_lui_mccain_ebc'
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
DAYS = pd.to_datetime(DAYS, utc=False).tz_localize(None)

# Plants and lines (referential integrity across tables)
PLANTS = [
    {'plant_id': 1, 'plant_name': 'NA-ID-P01', 'region': 'NA'},
    {'plant_id': 2, 'plant_name': 'NA-ME-P02', 'region': 'NA'},
    {'plant_id': 3, 'plant_name': 'EU-NL-P03', 'region': 'EU'},
    {'plant_id': 4, 'plant_name': 'EU-BE-P04', 'region': 'EU'},
]
# Lines per plant (ensure L2 at EU-NL-P03 and L3 at NA-ID-P01 exist)
LINES_PER_PLANT = {
    'NA-ID-P01': ['L1', 'L2', 'L3', 'L4'],
    'NA-ME-P02': ['L1', 'L2', 'L3'],
    'EU-NL-P03': ['L1', 'L2', 'L3', 'L4', 'L5'],
    'EU-BE-P04': ['L1', 'L2', 'L3', 'L4'],
}
# SKUs and numeric product_ids
PRODUCTS = [
    {'product_id': 9009, 'product_name': 'SC-9mm'},
    {'product_id': 1313, 'product_name': 'CC-13mm'},
    {'product_id': 2525, 'product_name': 'WG-25mm'},
]
VARIETIES = ['Russet', 'Innovator', 'Shepody', 'Bintje']

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
# max_usda_color_0..4 (int), max_defect_points (int), min_dry_matter_pct (double), max_dry_matter_pct (double),
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
    # USDA color caps (counts per sampling window ~ hour)
    color_caps = {
        'SC-9mm': (350, 1800, 900, 200, 30),
        'CC-13mm': (300, 1700, 1000, 250, 40),
        'WG-25mm': (380, 1600, 950, 240, 35),
    }
    # Defect points caps
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
                c0, c1, c2, c3, c4 = color_caps[pname]
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
                        'max_usda_color_0': int(c0),
                        'max_usda_color_1': int(c1),
                        'max_usda_color_2': int(c2),
                        'max_usda_color_3': int(c3),
                        'max_usda_color_4': int(c4),
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
    # Per plant daily loads: weekdays higher, weekends lower
    for plant in PLANTS:
        plant_name = plant['plant_name']
        plant_id = plant['plant_id']
        for d in DAYS:
            day = pd.Timestamp(d).normalize().floor('ms')
            weekday = day.weekday()
            base_loads = np.random.poisson(lam=10 if weekday < 5 else 6)
            base_loads = max(base_loads, 3)
            # Event impacts:
            # EU-NL-P03: Supplier S7 high-DM loads from 2025-08-17 onwards
            s7_share = 0.1
            if (plant_name == 'EU-NL-P03') and (day >= S7_TRANCHE_START):
                s7_share = 0.35  # more S7 loads
            # Build load entries
            for i in range(base_loads):
                is_s7 = np.random.rand() < s7_share
                supplier_code = 'S7' if is_s7 else np.random.choice(['S1', 'S2', 'S3', 'S4', 'S5', 'S6'])
                variety = np.random.choice(VARIETIES)
                # load_number encodes plant, date, seq, supplier
                seq = i + 1
                load_number = f"{plant_name}-{day.strftime('%Y%m%d')}-{seq:03d}-{supplier_code}"
                # Weight (kg) 12k..28k log-normal
                eff_wt = int(np.clip(np.random.lognormal(mean=np.log(18_000), sigma=0.35), 12_000, 28_000))
                # Average length grading by plant/product mix; keep within 50..85
                avg_len = int(np.clip(np.random.normal(70, 6), 50, 85))
                # USDA color % shares (0..100) sum to ~100
                # Better color distribution for higher DM
                if is_s7 and (plant_name == 'EU-NL-P03') and (day >= S7_TRANCHE_START):
                    dm = float(np.random.uniform(23.0, 24.0))
                    color1 = np.random.uniform(55, 70)
                    color0 = np.random.uniform(5, 15)
                    color2 = np.random.uniform(10, 20)
                else:
                    dm = float(np.random.lognormal(mean=np.log(21.5), sigma=0.06))
                    dm = float(np.clip(dm, 20.0, 24.5))
                    color1 = np.random.uniform(50, 65)
                    color0 = np.random.uniform(5, 15)
                    color2 = np.random.uniform(12, 22)
                color3 = np.random.uniform(0.5, 5.0)
                color4 = np.random.uniform(0.0, 1.2)
                # Normalize to 100
                total_color = color0 + color1 + color2 + color3 + color4
                color0 = color0 * 100.0 / total_color
                color1 = color1 * 100.0 / total_color
                color2 = color2 * 100.0 / total_color
                color3 = color3 * 100.0 / total_color
                color4 = color4 * 100.0 / total_color
                # Defect points
                base_def = np.random.randint(800, 1600)
                if (plant_name == 'NA-ID-P01') and (EVENT_START <= day <= EVENT_END):
                    # Under-quality period more defects
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
    # Build per plant_line daily segments (multiple entries per day, mix of runs and downtimes)
    for plant in PLANTS:
        plant_name = plant['plant_name']
        plant_id = plant['plant_id']
        lines = LINES_PER_PLANT[plant_name]
        for line in lines:
            for d in DAYS:
                day = pd.Timestamp(d).normalize()
                weekday = day.weekday()
                segments = np.random.poisson(lam=8 if weekday < 5 else 6)
                segments = max(segments, 3)
                # Distribution of product_name for the line
                prod_probs = np.array([0.45, 0.35, 0.20])
                prod_probs = prod_probs / prod_probs.sum()
                for s in range(segments):
                    # Determine start hour with business bias
                    if weekday < 5:
                        hour_probs = np.array([
                            0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.04, 0.06, 0.08, 0.09, 0.10, 0.10,
                            0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.03, 0.02, 0.02, 0.01, 0.01,
                        ])
                    else:
                        hour_probs = np.array([
                            0.01, 0.01, 0.01, 0.01, 0.015, 0.02, 0.03, 0.04, 0.06, 0.08, 0.09, 0.09,
                            0.08, 0.07, 0.06, 0.05, 0.05, 0.04, 0.035, 0.03, 0.025, 0.02, 0.015, 0.01,
                        ])
                    hour_probs = hour_probs / hour_probs.sum()
                    start_hour = int(np.random.choice(np.arange(24), p=hour_probs))
                    start_min = int(np.random.randint(0, 60))
                    start_time = (day + pd.Timedelta(hours=start_hour, minutes=start_min)).floor('ms')
                    # Duration seconds with heavy-tail
                    dur_sec = int(np.random.lognormal(mean=np.log(3600), sigma=0.6))  # ~1h avg
                    dur_sec = int(np.clip(dur_sec, 600, 18_000))
                    end_time_ts = (start_time + pd.Timedelta(seconds=dur_sec)).floor('ms')
                    # end_time string with occasional format issues
                    if np.random.rand() < 0.15:
                        # Different string format
                        end_time_str = end_time_ts.strftime('%m/%d/%Y %H:%M:%S')
                    elif np.random.rand() < 0.05:
                        # Missing seconds
                        end_time_str = end_time_ts.strftime('%Y-%m-%d %H:%M')
                    else:
                        end_time_str = end_time_ts.strftime('%Y-%m-%d %H:%M:%S')
                    # Product for this segment
                    prod_idx = int(np.random.choice(len(PRODUCTS), p=prod_probs))
                    product_name = PRODUCTS[prod_idx]['product_name']
                    # Downtime categorization
                    is_downtime = np.random.rand() < (0.20 if weekday < 5 else 0.25)
                    dt_cat_1 = 'Maintenance' if is_downtime and (np.random.rand() < 0.5) else ('Cleaning' if is_downtime else 'Run')
                    dt_cat_2 = 'Cutter' if is_downtime else 'N/A'
                    # Inject specific downtime codes on NA-ID-P01 L3 around 2025-08-22..
                    notes_code = ''
                    if (plant_name == 'NA-ID-P01') and (line == 'L3'):
                        if pd.Timestamp('2025-08-22') <= day <= pd.Timestamp('2025-09-05') and is_downtime:
                            # Emulate codes DOW-PI-4521, DOW-PI-4574 in the cat_2 text
                            dt_cat_2 = np.random.choice(['Cutter DOW-PI-4521', 'Cutter DOW-PI-4574', 'Fryer'])
                    # Downtime duration spikes during event window (under-quality)
                    dt_duration = int(np.random.randint(0, int(dur_sec * (0.6 if is_downtime else 0.15))))
                    if (plant_name == 'NA-ID-P01') and (line == 'L3') and (EVENT_START <= day <= EVENT_END):
                        # Increase downtime during event
                        if is_downtime:
                            dt_duration = int(np.clip(dt_duration + np.random.randint(600, 3600), 300, dur_sec))
                    # Qty packed correlates negatively with downtime
                    base_rate = np.random.uniform(4.0, 7.5)  # kg per sec approx scaled
                    qty_packed = float(max(0.0, base_rate * (dur_sec - dt_duration)))
                    # Totes on ~ throughput indicator
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
    # Allow a tiny null fraction in num_totes_on (~0.2%)
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
    # Hourly events with weekday concentration
    for plant in PLANTS:
        plant_id = plant['plant_id']
        plant_name = plant['plant_name']
        region = plant['region']
        for d in DAYS:
            day = pd.Timestamp(d).normalize()
            weekday = day.weekday()
            # Hours per day: weekdays more; weekends less
            hours_count = np.random.poisson(lam=18 if weekday < 5 else 12)
            hours_count = int(np.clip(hours_count, 8, 22))
            # Pick hours
            if weekday < 5:
                hour_probs = np.array([
                    0.01, 0.01, 0.01, 0.01, 0.015, 0.02, 0.03, 0.05, 0.08, 0.09, 0.10, 0.10,
                    0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.03, 0.02, 0.02, 0.015, 0.01,
                ])
            else:
                hour_probs = np.array([
                    0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.04, 0.06, 0.08, 0.09, 0.09,
                    0.08, 0.07, 0.06, 0.05, 0.05, 0.045, 0.04, 0.035, 0.03, 0.02, 0.015, 0.01,
                ])
            hour_probs = hour_probs / hour_probs.sum()
            hours = np.random.choice(np.arange(24), size=hours_count, replace=False, p=hour_probs)
            # Product mix per plant
            prod_probs = np.array([0.45, 0.35, 0.20])
            prod_probs = prod_probs / prod_probs.sum()
            prods = np.random.choice([p['product_name'] for p in PRODUCTS], size=hours_count, p=prod_probs)
            for hh, pname in zip(hours, prods):
                # Base quality distributions
                # avg_length_mm ~ product target +/- random
                avg_len_target = {'SC-9mm': 60, 'CC-13mm': 70, 'WG-25mm': 80}[pname]
                avg_length_mm = int(np.clip(np.random.normal(avg_len_target, 4), 50, 85))
                # USDA counts baseline heavy-tail
                base_total = np.random.randint(3500, 6500)  # total fries sampled per hour
                # Distribute counts across bins with shares
                if pname == 'CC-13mm' and plant_name == 'NA-ID-P01' and (EVENT_START <= day <= EVENT_END):
                    # Under-quality spike: color 2 increases, defects rise
                    shares = np.array([0.06, 0.50, 0.30, 0.11, 0.03])
                    defect_points = np.random.randint(2600, 3800)
                    dry_solids = float(np.random.uniform(20.6, 21.0))
                elif pname == 'SC-9mm' and plant_name == 'EU-NL-P03' and (EVENT_START <= day <= EVENT_END):
                    # Over-quality sustained: high dry solids (23.4..23.9), within defects
                    shares = np.array([0.08, 0.62, 0.23, 0.06, 0.01])
                    defect_points = np.random.randint(1200, 2000)
                    dry_solids = float(np.random.uniform(23.4, 23.9))
                else:
                    shares = np.array([0.07, 0.65, 0.20, 0.07, 0.01])
                    defect_points = np.random.randint(1200, 2400)
                    # Baseline dry solids by SKU region
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
                        'product_id': str(next(p['product_id'] for p in PRODUCTS if p['product_name'] == pname)),
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
    # Normalize datetime to ms
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce').dt.floor('ms')
    # Tiny null rate in variety (~0.2%)
    mask_null = np.random.rand(len(df)) < 0.002
    df.loc[mask_null, 'variety'] = None
    return df

# ===============================
# === SUMMARY / QA ASSERTIONS ===
# ===============================

def print_story_signals_summary(osipi: pd.DataFrame, loads: pd.DataFrame, oee: pd.DataFrame):
    print('\nQuick QA summary:')
    # EU-NL-P03 L2 SC-9mm over-quality in OSI PI
    # Ensure datetime is proper dtype for comparisons
    osipi['datetime'] = pd.to_datetime(osipi['datetime'], errors='coerce').dt.floor('ms').dt.tz_localize(None)
    mask_over = (
        (osipi['plant_name'] == 'EU-NL-P03')
        & (osipi['product_name'] == 'SC-9mm')
        & (osipi['datetime'] >= EVENT_START)
        & (osipi['datetime'] <= EVENT_END)
    )
    over_dm = osipi.loc[mask_over, 'dry_solids_pct']
    print(f"  EU-NL-P03 SC-9mm event DM avg: {over_dm.mean():.2f}% (n={len(over_dm)})")

    # NA-ID-P01 L3 CC-13mm under-quality & defects
    osipi['datetime'] = pd.to_datetime(osipi['datetime'], errors='coerce').dt.floor('ms').dt.tz_localize(None)
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
    loads['date'] = loads['load_number'].str.split('-').str[1]
    loads['date'] = pd.to_datetime(loads['date'], format='%Y%m%d', errors='coerce')
    mask_s7 = (loads['plant_name'] == 'EU-NL-P03') & (loads['load_number'].str.contains('S7')) & (loads['date'] >= S7_TRANCHE_START)
    s7_dm = loads.loc[mask_s7, 'dry_matter_pct']
    print(f"  EU-NL-P03 S7 loads DM avg post-tranche: {s7_dm.mean():.2f}% (n={len(s7_dm)})")

    # OEE: downtime spike NA-ID-P01 L3 during event
    # Ensure OEE datetime is proper dtype for comparisons
    oee['datetime'] = pd.to_datetime(oee['datetime'], errors='coerce').dt.floor('ms').dt.tz_localize(None)
    oee['date'] = oee['datetime']
    mask_oee = (
        (oee['plant_name'] == 'NA-ID-P01')
        & (oee['plant_line'] == 'L3')
        & (oee['date'] >= EVENT_START)
        & (oee['date'] <= EVENT_END)
        & (oee['downtime_cat_1'] != 'Run')
    )
    print(f"  OEE downtime events NA-ID-P01 L3 during event: {mask_oee.sum():,}")

# ===============================
# === MAIN
# ===============================
if __name__ == '__main__':
    print('Starting McCain Foods RAW data generation...')
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

    # QA summary
    print_story_signals_summary(raw_osipi, raw_loads, raw_oee)

    print('\n' + '=' * 60)
    print('GENERATION COMPLETE - All timestamps are naive, floored to ms.')
    print('=' * 60)
