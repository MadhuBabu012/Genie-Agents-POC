-- Metric views for the retail medallion project (Day 5 of the brief).
--
-- The brief requires the business metrics (revenue, units sold, return rate,
-- sell-through, stock-out rate) to be defined ONCE, in metric views, and not
-- repeated in Genie instructions. Metric views are Unity Catalog objects
-- whose definitions are written in YAML, wrapped in a
-- CREATE VIEW ... WITH METRICS LANGUAGE YAML statement. They are NOT Spark
-- Declarative Pipeline datasets, which is why they are not part of the
-- pipeline's Python transformation files.
--
-- Run each statement below on a SQL warehouse to (re)create the metric views
-- in the gold schema. The pipeline's catalog/schema parameters live in
-- common.py (target_catalog = genie_agents_poc, gold_schema = gold);
-- to repoint at another catalog, replace genie_agents_poc.gold below.

-- ======================================================================
-- 1) Sales metrics: revenue, units sold, return rate, sell-through,
--    clearance share and average discount, sliceable by date, week, month,
--    region, store, grade, category, style, size, season.
-- ======================================================================
CREATE OR REPLACE VIEW genie_agents_poc.gold.mv_retail_sales
WITH METRICS LANGUAGE YAML AS
$$
version: 1.1
comment: >-
  Retail sales metrics over gold fact_sales. Grain: one row per store x
  product x day; measures aggregate over the dates selected in the query.
  Returns are real business events: they reduce revenue and count in
  units_returned. stock (fact_store_stock) is joined at store x product to
  carry the clearance flag and the as-of stock on hand for sell-through.
source: genie_agents_poc.gold.fact_sales

joins:
- name: store
  source: genie_agents_poc.gold.dim_store
  'on': source.store_key = store.store_key
  rely:
    at_most_one_match: true
- name: product
  source: genie_agents_poc.gold.dim_product
  'on': source.product_key = product.product_key
  rely:
    at_most_one_match: true
- name: stock
  source: genie_agents_poc.gold.fact_store_stock
  'on': source.store_key = stock.store_key AND source.product_key = stock.product_key
  rely:
    at_most_one_match: true

fields:
- name: bill_date
  expr: source.bill_date
  comment: 'POS bill date'
- name: bill_week
  expr: DATE_TRUNC('WEEK', source.bill_date)
  comment: 'Monday of the bill week'
  synonyms:
  - week
- name: bill_month
  expr: DATE_TRUNC('MONTH', source.bill_date)
  comment: 'First day of the bill month'
  synonyms:
  - month
- name: region
  expr: store.region
- name: state
  expr: store.state
- name: city
  expr: store.city
- name: store_name
  expr: store.store_name
  synonyms:
  - store
- name: store_code
  expr: store.store_code
- name: store_grade
  expr: store.grade
  synonyms:
  - grade
- name: category
  expr: product.category
- name: style_name
  expr: product.style_name
  synonyms:
  - style
- name: colour
  expr: product.colour
- name: size
  expr: product.size
  synonyms:
  - sizes
- name: size_class
  expr: product.size_class
- name: is_pivotal
  expr: product.is_pivotal
  comment: 'True for core sizes the replenishment engine protects'
- name: season
  expr: product.season
- name: is_clearance
  expr: stock.is_clearance
  comment: 'True if the style is on end-of-season clearance'

measures:
- name: revenue
  expr: SUM(source.net_amount)
  comment: 'Net revenue: sales minus returns, after discount'
  display_name: Revenue
  format:
    type: currency
    currency_code: INR
    decimal_places:
      type: exact
      places: 2
  synonyms:
  - net sales
  - sales value
- name: gross_revenue
  expr: SUM(source.gross_amount)
  comment: 'Gross revenue before discount, sales only'
  display_name: Gross Revenue
  format:
    type: currency
    currency_code: INR
    decimal_places:
      type: exact
      places: 2
- name: units_sold
  expr: SUM(source.units_sold)
  comment: 'Units sold, returns excluded'
  display_name: Units Sold
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
  synonyms:
  - sales units
- name: units_returned
  expr: SUM(source.units_returned)
  comment: 'Units returned (negative POS lines counted positively)'
  display_name: Units Returned
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
- name: return_rate
  expr: SUM(source.units_returned) / NULLIF(SUM(source.units_sold), 0)
  comment: 'Units returned per unit sold'
  display_name: Return Rate
  format:
    type: percentage
    decimal_places:
      type: all
  synonyms:
  - returns rate
- name: avg_discount_pct
  expr: 100.0 * SUM(source.discount_amount) / NULLIF(SUM(source.gross_amount), 0)
  comment: 'Discount as a share of gross revenue'
  display_name: Average Discount
  format:
    type: percentage
    decimal_places:
      type: all
  synonyms:
  - discount rate
- name: clearance_revenue_share
  expr: SUM(source.net_amount) FILTER (WHERE stock.is_clearance) / NULLIF(SUM(source.net_amount), 0)
  comment: 'Share of net revenue from end-of-season clearance styles'
  display_name: Clearance Revenue Share
  format:
    type: percentage
    decimal_places:
      type: all
- name: sell_through
  expr: SUM(source.units_sold) / NULLIF(SUM(source.units_sold) + SUM(stock.stock_on_hand), 0)
  comment: >-
    Units sold in the selected period divided by units sold plus the units on
    hand at the as-of stock snapshot - how much of what was available was sold.
  display_name: Sell-Through
  format:
    type: percentage
    decimal_places:
      type: all
  synonyms:
  - sell through rate
$$

-- ======================================================================
-- 2) Store stock-position metrics: stock-out rate, units on hand, in
--    transit, warehouse availability and units needed, at the as-of
--    snapshot date (one row per store x product).
-- ======================================================================
CREATE OR REPLACE VIEW genie_agents_poc.gold.mv_store_stock
WITH METRICS LANGUAGE YAML AS
$$
version: 1.1
comment: >-
  Store stock-position metrics over gold fact_store_stock. Grain: one row per
  store x product on the as-of snapshot date. A position is stocked out when
  stock_on_hand is zero.
source: genie_agents_poc.gold.fact_store_stock

joins:
- name: store
  source: genie_agents_poc.gold.dim_store
  'on': source.store_key = store.store_key
  rely:
    at_most_one_match: true
- name: product
  source: genie_agents_poc.gold.dim_product
  'on': source.product_key = product.product_key
  rely:
    at_most_one_match: true

fields:
- name: snapshot_date
  expr: source.snapshot_date
  comment: 'As-of date of the stock snapshot'
- name: region
  expr: store.region
- name: state
  expr: store.state
- name: city
  expr: store.city
- name: store_name
  expr: store.store_name
  synonyms:
  - store
- name: store_code
  expr: store.store_code
- name: store_grade
  expr: store.grade
  synonyms:
  - grade
- name: store_status
  expr: store.status
  comment: 'Open, Hold or Closed'
- name: category
  expr: product.category
- name: style_name
  expr: product.style_name
  synonyms:
  - style
- name: size
  expr: product.size
- name: size_class
  expr: product.size_class
- name: is_pivotal
  expr: product.is_pivotal
- name: is_on_hold
  expr: source.is_on_hold
  comment: 'True if the style is blocked from replenishment'
- name: hold_reason
  expr: source.hold_reason
- name: is_clearance
  expr: source.is_clearance
  comment: 'True if the style is on end-of-season clearance'

measures:
- name: stock_out_rate
  expr: AVG(CASE WHEN source.stock_on_hand <= 0 THEN 1.0 ELSE 0.0 END)
  comment: 'Share of store-product positions with zero stock on hand'
  display_name: Stock-Out Rate
  format:
    type: percentage
    decimal_places:
      type: all
  synonyms:
  - out of stock rate
- name: stocked_out_positions
  expr: SUM(CASE WHEN source.stock_on_hand <= 0 THEN 1 ELSE 0 END)
  comment: 'Number of store-product positions with zero stock'
  display_name: Stocked-Out Positions
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
- name: units_on_hand
  expr: SUM(source.stock_on_hand)
  comment: 'Units currently on hand in stores'
  display_name: Units On Hand
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
- name: units_in_transit
  expr: SUM(source.in_transit_qty)
  comment: 'Units dispatched to stores, not yet received'
  display_name: Units In Transit
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
- name: warehouse_available_units
  expr: SUM(source.wh_available)
  comment: 'Units available in the serving warehouses'
  display_name: Warehouse Available Units
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
- name: units_needed
  expr: SUM(source.units_needed)
  comment: 'Units the replenishment engine still needs to send'
  display_name: Units Needed
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
- name: positions
  expr: COUNT(1)
  comment: 'Number of store-product stock positions'
  display_name: Stock Positions
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
$$

-- ======================================================================
-- 3) Warehouse stock metrics: availability by day, warehouse and product,
--    including the warehouse stock-out rate (the WH-SOUTH story).
-- ======================================================================
CREATE OR REPLACE VIEW genie_agents_poc.gold.mv_warehouse_stock
WITH METRICS LANGUAGE YAML AS
$$
version: 1.1
comment: >-
  Warehouse stock metrics over gold fact_warehouse_stock. Grain: one row per
  day x warehouse x product. A warehouse-product-day is out of stock when
  available_qty is zero.
source: genie_agents_poc.gold.fact_warehouse_stock

joins:
- name: product
  source: genie_agents_poc.gold.dim_product
  'on': source.product_key = product.product_key
  rely:
    at_most_one_match: true

fields:
- name: snapshot_date
  expr: source.snapshot_date
  comment: 'Warehouse stock day'
  synonyms:
  - date
- name: warehouse
  expr: source.warehouse
- name: category
  expr: product.category
- name: style_name
  expr: product.style_name
  synonyms:
  - style
- name: size
  expr: product.size
- name: size_class
  expr: product.size_class
- name: is_pivotal
  expr: product.is_pivotal
  comment: 'True for core sizes the replenishment engine protects'

measures:
- name: available_units
  expr: SUM(source.available_qty)
  comment: 'Units available in warehouses'
  display_name: Available Units
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
- name: warehouse_stock_out_rate
  expr: AVG(CASE WHEN source.available_qty <= 0 THEN 1.0 ELSE 0.0 END)
  comment: 'Share of warehouse-product-days with zero available stock'
  display_name: Warehouse Stock-Out Rate
  format:
    type: percentage
    decimal_places:
      type: all
  synonyms:
  - warehouse out of stock rate
- name: zero_stock_product_days
  expr: SUM(CASE WHEN source.available_qty <= 0 THEN 1 ELSE 0 END)
  comment: 'Number of warehouse-product-days with zero available stock'
  display_name: Zero-Stock Product-Days
  format:
    type: number
    decimal_places:
      type: exact
      places: 0
$$
