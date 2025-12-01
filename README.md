Building ELT Data Pipelines with Airflow and dbt
======================================================

Aim
----
Build production-ready ELT data pipelines with Apache Airflow and dbt Cloud. Process Airbnb and Census data for Sydney, load it into Postgres using a medallion architecture (Bronze, Silver, Gold), and publish a data mart for analytical insights. Deliver ad-hoc analyses that answer defined business questions.

Datasets
--------
- **Airbnb (Sydney, May 2020–April 2021):** Listings, availability, pricing, hosts, neighbourhoods. Source link provided in brief.
- **Census (ABS G01 & G02 at LGA level):** Selected person characteristics and medians/averages. Source link provided in brief.
- **LGA/Suburb mapping:** Codes to join Airbnb and Census data. Source link provided in brief.

Tasks and Process
-----------------
### Part 0: Download data
- 12 months of Airbnb listings for Sydney (May 2020–April 2021).
- Census tables G01 and G02 at LGA level.
- LGA code and suburb mapping file.

### Part 1: Airflow → Postgres (Bronze)
1) Upload to Airflow bucket: first month Airbnb file (`05_2020.csv`), Census datasets, and LGA mapping.  
2) In Postgres, create Bronze schema and raw tables (via DBeaver).  
3) Build an Airflow DAG (`schedule_interval=None`) that reads from the bucket and loads raw tables in Bronze.

### Part 2: Warehouse design with dbt (Bronze, Silver, Gold)
- **Bronze:** Raw tables loaded from Airflow.  
- **Silver:** Cleaned/standardized tables; decompose listings into entities and snapshot dimension candidates using timestamp strategy. Fix listing date/LGA issues; apply consistent naming. Snapshots only for dimension-bound tables (not facts).  
- **Gold:**  
  - Star schema with fact tables containing IDs/metrics (e.g., price).  
  - **Datamart views** (join fact to dimensions with SCD2 logic so facts align to dimension state at that time):
    - `dm_listing_neighbourhood` (per neighbourhood + month/year): active listing rate, min/max/median/avg price for active listings, distinct hosts, superhost rate, avg review score (active listings), pct change active, pct change inactive, total stays, avg estimated revenue per active listing; order by neighbourhood, month/year.
    - `dm_property_type` (per property_type, room_type, accommodates + month/year): same metrics as above; order by property_type, room_type, accommodates, month/year.
    - `dm_host_neighbourhood` (per host_neighbourhood_lga + month/year): distinct hosts, avg estimated revenue per active listing, estimated revenue per host; order by host_neighbourhood_lga, month/year.

Metrics and Definitions
-----------------------
- Active listing: `has_availability = 't'`.  
- Active listing rate = (active listings / total listings) * 100.  
- Superhost rate = (distinct hosts with `host_is_superhost = 't'` / total distinct hosts) * 100.  
- Percentage change (month to month) = ((final - original) / original) * 100.  
- Number of stays (active listings) = `31 - availability_30`.  
- Estimated revenue per active listing = stays * price.  
- Estimated revenue per host = total estimated revenue per active listing / total distinct hosts.

### Part 3: Load remaining Airbnb data
- Run the Airflow DAG month-by-month in chronological order; trigger dbt job after each load to preserve sequencing and integrity.

### Part 4: Ad-hoc analysis (Postgres SQL)
Provide SQL answers and results (with screenshots) for:
1) Demographic differences (age distribution, household size, etc.) between top 1/3 and bottom 1/3 LGAs by estimated revenue per active listing over last 12 months.  
2) Correlation between neighbourhood median age (Census) and revenue per active listing.  
3) Best listing type (property type, room type, accommodates) for the top 15 listing_neighbourhoods (by avg estimated revenue per active listing) to maximize number of stays.  
4) For multi-listing hosts in Vic: are listings concentrated in the same LGA or spread across LGAs?  
5) For single-listing hosts in Vic: does revenue over the last 12 months cover the annualized median mortgage repayment in the LGA? Which LGA has the highest percentage of hosts that can cover it?

