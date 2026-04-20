# Arcan Weekly Reports — Project Instructions

## Overview

This project automates the generation of weekly property reports for Arcan Capital. Every Monday, Asset Living sends Yardi source files for ~20 multifamily properties. These files are parsed, combined with historical data from a database, and used to fill Excel report templates. There are two templates: a **Short Template** for Arcan-owned properties and a **Long Template** for third-party managed properties (e.g., Latitude).

---

## Property Roster

### Short Template (Arcan-Owned — Internal Use)
| Property | Code | Yardi # |
|---|---|---|
| Woodland Commons | wdlndcm | 1121 |
| The Turn | turn | 1122 |
| Portico at Lanier | portico | 1119 |
| Georgetown | georget | 1120 |
| Longview | longvw | 1110 |
| Kensington Place | kenplc | 1109 |
| Perry Heights | perryh | 1113 |
| Abbey Lake | abbeylk | 1102 |
| Marbella | marbla | 1117 |

### Long Template (Third-Party Managed — External Use)
| Property | Code | Yardi # |
|---|---|---|
| Manchester at Wesleyan | manwes | 1116 |
| Tapestry Park | tapeprk | 1118 |
| Haven | haven | 1108 |
| Hamptons at East Cobb | hampec | 1126 |
| Tall Oaks | talloak | 1125 |
| Colony Woods | colwds | 1106 |
| Marsh Point | marshp | 1111 |
| Capella | capella2 | 1104 |
| 55 Pharr | 55pharr | 1101 |
| Emerson 1600 | emersn | 1107 |

### Other Properties (not in weekly reports)
| Property | Code | Notes |
|---|---|---|
| The Hangar | hangar | Arcan property, separate reporting |
| Embry Townhomes | embry | No active reporting |
| Reeves Office | reeves | Office, not residential |

---

## Source Data

### 1. Yardi Reports (12 per property, received every Monday from Asset Living)
All files arrive in a single zip, mixed together. Files are identified by property code in the filename (e.g., `_perryh.`, `_55pharr.`).

| File Pattern | Used By | Data Extracted |
|---|---|---|
| `ResAnalytics_Box_Score_Summary_{code}.xlsx` | Both | Occupancy %, leased %, units, occupied, vacant, notice, available, unit types, move-ins/outs |
| `ResAnalytic_Lease_Expiration_{code}.xlsx` | Both | MTM count, 12-month expiration counts |
| `ResAnalytics_Conversion_Ratios_{code}.xlsx` | Both | New leads (calls+walk-in+email+SMS+web+chat), tours/shows, applications, approved/denied/cancelled |
| `ResARAnalytics_Delinquency_Summary_{code}.xlsx` | Short only | Aging buckets (0-30, 31-60, 61-90, 90+), prepayments, total balance |
| `ResARAnalytics_Delinquency_Summary_{code}_2.xlsx` | Short only | Detail rows — count "not filed" (status=Current AND 0-30 owed > $1,000) |
| `Residents_on_Notice_{code}.xlsx` | Both | Eviction count (status="Eviction"), notice count (status="Notice") |
| `Pending_Make_Ready_Unit_Details__{code}.xlsx` | Both | Count of pending make-ready units (rows with property code + date) |
| `Work_Order_Report_{code}.xlsx` | Both | Count of open work orders (rows with numeric WO#) |
| `Budget_Comparison_with_PTD__{code}_Accrual_AJEs_Modified_Accrual.xlsx` | Long only | Financial P&L: income and expense line items with MTD Actual, MTD Budget, PTD Actual, PTD Budget. Matched by account code. |
| `ResAnalytics_Market_Rent_Schedule_{code}.xlsx` | Long only | Average market rent, occupied rent, sqft, unit counts |
| `ResAnalytics_Unit_Availability_Details_{code}.xlsx` | Long only | Unit-level detail: type, rent, sqft, status, lease dates |
| `ProjectedOccupancy{date}.xlsx` | Both | 6 weeks of scheduled move-ins/outs. One file may cover multiple properties. |

### 2. Emily Patton's Cash Report (weekly email, prior week)
- **Subject:** "Cash Report"
- **Format:** Single Excel file covering all properties
- **Columns:** Property name, Bank Statement Balance, DITs, Items not in Book, Outstanding Checks, Ending Balance, Status, Available Balance, Book Balance, Notes
- **Maps to:** Long template INPUT sheet Z-AB (Actual Cash = Book Balance, Adjusted Cash = Available Balance)
- **Filename format:** `MM_DD_YYYY.xlsx` (date in filename)

### 3. Luke Mills' Tuesday Summary (currently manual, to be automated)
- **Sent:** Monday afternoon before Tuesday exec meeting
- **Format:** Color-coded table embedded in email with two embedded images
- **Columns:** Property, Units, Occupancy, Pre-Leased, Collections, ACE, Status
- **Status categories:** No Talk (green), OTV (yellow), Meh (orange), Issue (red)
- **ACE formula:** Unknown — need to get from Luke
- **Goal:** Auto-generate this from the same data processed Monday

---

## Database

### Location
GitHub repository: `shivaanik123/arcan-reports-api` (data/ folder)
- Access via git clone/push using Personal Access Token
- Token stored in `github_db.py`

### Also deployed on Railway (backup, not currently accessible from Claude)
- Railway API: `web-production-03c9c.up.railway.app`
- Postgres: `shinkansen.proxy.rlwy.net:55881/railway`
- Blocked by Claude's network restrictions — pending domain whitelisting from Anthropic support

### Tables (CSV files in GitHub)

**properties.csv**
- property_code, name, units, models, office, location, equity_investment, analysis_start

**historical_occupancy.csv**
- id, property_code, date, occupancy, leased, projection, make_ready, work_orders, turnover
- Weekly records for short template properties, monthly for long template
- Occupancy stored as percentage (e.g., 97.05 = 97.05%)

**historical_financial.csv**
- id, property_code, date, market_rent, occupied_rent, revenue, expenses, owed, charges, collections, income_actual, income_budget, expense_actual, expense_budget, actual_cash, adjusted_cash, collected, collections_pct
- Monthly records
- collections_pct stored as percentage (e.g., 94.2 = 94.2%)

**weekly_data.csv**
- id, property_code, week_date, occupancy_pct, leased_pct, units, occupied, vacant, notice, rented, available, move_ins, move_outs, evictions, make_ready_count, work_orders_count, new_leads, tours, applications, delinquency_0_30 through delinquency_over_90, prepayment, delinquency_balance, evictions_filed, evictions_not_filed

**monthly_data.csv**
- id, property_code, month_date, expirations, mtm, market_rent, occupied_rent, revenue, expenses, owed, charges, collections, pct_collected, renewals, move_outs, new_leads, tours, applications, move_ins

---

## Templates

### Short Template (`Weekly_Report_Template_Clean.xlsx`)
**Sheets:** Occupancy, Financial

#### Occupancy Sheet
- **B2-B8:** Property info (name, units, models, office, analysis start, location, equity investment) — from database
- **F1, C3:** Report date
- **Row 17:** Current occupancy (%, leased %, units, occupied, vacant, notice, rented) — from Box Score
- **Row 19:** 30-day projected (projected occ %, available, move-ins, move-outs, evictions) — calculated + Residents on Notice
- **Rows 22+:** Unit vacancy detail (one row per unit type) — from Box Score
- **Cols N-S, Rows 22+:** Historical occupancy (date, occ %, leased %, projection %, make ready, work orders) — from database. Only write rows where occupancy is not null.
- **Chart:** 1 occupancy trend chart reading N-Q, last 56 weeks. Y-axis 75%-100%.

#### Financial Sheet
- **J3:K14:** Lease expiration lookup table (date, count) — formulas in G16:H27 use VLOOKUPs against this
- **E28:** Current month name in caps
- **Rows 29-33:** Leasing activity — Col C (this week from Yardi), Col D (last week from DB), Col E (current month sum from DB), Col F (last month from DB)
- **H35-H40:** Delinquency aging — from Delinquency Summary
- **Rows 46-47:** Maintenance — Col C (this week), Col D-F (last 1/2/3 weeks from DB)
- **H46-H48:** Evictions filed, not filed, MTM
- **Cols M-T, Rows 3+:** Historical financial (date, market rent, occupied rent, revenue, expenses, owed, charges, collections %) — from database
- **Charts:** 3 charts (rent trend, revenue/expenses, collections %)

### Long Template (`Long_Report_Template.xlsx`)
**Sheets:** INPUT, Cover, Occ, Fin, Col, Rent Cash
**Key principle:** ALL data goes into INPUT sheet only. All other sheets use formulas referencing INPUT.

#### INPUT Sheet Cell Map

**Basic Info (A-C, rows 2-6):**
- C2: Report date, C3: Property name, C4: Location, C5: Units, C6: Week ending date

**Current Occupancy (rows 8-14):**
- C8: Total occupied, C9: Model/down, C10: Vacant rentable, C11: Leased vacant, C12: Notice, C13: Units under eviction, C14: Pre-leased notice/eviction

**Leasing Activity (rows 16-20):**
- C16: Total traffic, C17: Total applications, C18: Approved, C19: Cancelled, C20: Denied

**Scheduled Move Ins/Outs (rows 24-29):**
- A: Date, B: Move-ins, C: Move-outs — from Projected Occupancy (6 weeks)

**Lease Expirations (rows 32-44):**
- A: Month, B: Expirations — from Lease Expiration report
- C: Renewed, D: Notice/Eviction, E: Move-out — source TBD (waiting on answer)

**Financial — 3 periods (cols F-K):**
- F-G: Current month (MTD from Budget Comparison)
- H-I: Last month (from database)
- J-K: 2 months ago (from database)
- F1, H1, J1: Period labels (e.g., "FEBRUARY 2026")
- Rows mapped by account code from Budget Comparison:

| Account Code | Line Item | INPUT Row |
|---|---|---|
| 41999 | Net Rental Income | 3 |
| 42999 | Other Income | 4 |
| 51199 | Payroll & Benefits | 8 |
| 51299 | Management Fee | 9 |
| 51399 | General & Admin | 10 |
| 52199 | Utilities | 11 |
| 53299 | Repairs & Maintenance | 12 |
| 53399 | Contract Services | 13 |
| 53499 | Make Ready | 14 |
| 53599 | Recreation Amenities | 15 |
| 56999 | Advertising & Marketing | 16 |
| 57199 | Taxes & Insurance | 17 |
| 59999 | NOI | 20 |
| 61990 | Debt Service | 22 |
| 81999 | Routine Replacement | 23 |
| 99990 | Net Income for Tax | 26 |

**Historical Occupancy (cols M-N, rows 2-266):**
- M: Date, N: Occupancy % — only rows with actual values (no gaps)

**Historical Turnover (cols P-Q, rows 2-80):**
- P: Date, Q: Turnover % — only rows with values

**Income vs Budget (cols S-U, rows 3-70):**
- S: Date, T: Actual income, U: Budget income

**Expense vs Budget (cols W-X, rows 3-70):**
- W: Actual expense, X: Budget expense (uses the same S date column as Income)

**Cash Balance (cols Z-AB, rows 3-60):**
- Z: Date, AA: Actual (Book Balance), AB: Adjusted (Available Balance)
- Current week from Emily's cash report appended at end

**Collections (cols AD-AG, rows 3-71):**
- AD: Date, AE: Charges, AF: Collected, AG: % collected
- Only write rows where charges or collected have values

**Unit Rent Roll (cols AI-AO, rows 3-42):**
- AI: Unit type code, AJ: Type name, AK: # Units, AL: # Occupied, AM: Sq ft, AN: Market rent, AO: In-place rent
- Grouped by unit type from Unit Availability Details

**Historical Rents (cols AP-AR, rows 3-15):**
- AP: Date, AQ: Market rent avg, AR: In-place rent avg

**Arcan Internal (cols AT-AV, rows 3+):**
- AT: Date, AU: Work orders, AV: Make readies

---

## Property Name → Code Mapping

Used for Emily's cash report, Luke's summary, and any name-based lookups:

```
55 pharr → 55pharr          abbey lake → abbeylk
capella → capella2           colony woods → colwds
emerson 1600 → emersn       georgetown → georget
hamptons at east cobb → hampec    the hangar → hangar
the haven → haven            kensington place → kenplc
longview meadow → longvw    longview → longvw
manchester at wesleyan → manwes   marbella → marbla
marsh point → marshp         perry heights → perryh
portico at lanier → portico  tall oaks → talloak
tapestry park → tapeprk     the turn → turn
woodland commons → wdlndcm
```

---

## Scripts

All scripts live in `/home/claude/` during execution:

| Script | Purpose |
|---|---|
| `github_db.py` | Database layer — pull/push CSV data from GitHub repo |
| `parse_long_report.py` | Parses the INPUT sheet of a long template weekly report and returns historical occupancy + financial rows. Used for both backfill and Monday incremental updates. |
| `generate_short_report.py` | Short template generator — parses Yardi files, fills template, writes back to DB |
| `generate_long_report.py` | Long template generator — same for long template, includes cash report integration |
| `orchestrator.py` | Main entry point — unzips bulk download, sorts by property, routes to correct generator, saves DB |

---

## Weekly Workflow (Target State)

### Monday (automated via Cowork, once fully set up)
1. **Detect** Asset Living's email in Outlook (Microsoft 365 MCP) → download zip attachment
2. **Detect** Emily Patton's cash report email → download Excel attachment
3. **Pull** database from GitHub
4. **Unzip** bulk download → sort ~220 files by property code
5. **For each property:**
   - Route to short or long template based on property code
   - Parse 11-12 Yardi source files
   - Pull historical data from database
   - Fill template
   - Save current week's data back to database
6. **Push** updated database to GitHub
7. **Generate** Luke's Tuesday summary table
8. **Present** all reports for review
9. **On approval:** Upload reports to Box (correct folders per property) and generate PDFs

### Until Outlook is connected:
- User uploads the zip file and cash report manually
- Everything else is automated

---

## Box Folder Structure

Reports are stored in: `05 Reporting → 07 Arcan Client Reports → Weekly Reports`
- Folder ID: `7627186266`

### Latitude properties subfolder:
- `03 Latitude Weekly Reports` (ID: `153748792833`)
  - `55 Pharr` (ID: `153749009240`)
  - `Capella` (ID: `153748487615`)
  - `Emerson 1600` (ID: `153747121667`)
  - `Marsh Point` (ID: `153748442082`)

### Other long template properties have individual folders under Weekly Reports:
- `HEC - Hamptons at East Cobb` (ID: `316639043030`)
- `HAH - Haven` (ID: `139647724800`)
- `MWM - Manchester at Wesleyan` (ID: `226663213356`)
- etc.

### Short template properties also have folders:
- `PHP - Perry Heights` (ID: `139647211915`)
- `KPG - Kensington Place` (ID: `11422600309`)
- `LMA - Longview Meadow` (ID: `7639070990`)
- `MGS - Marbella` (ID: `250808022967`)
- etc.

---

## Backfill Status

All 19 properties backfilled as of April 2026. The DB holds 2,651 occupancy rows and 1,018 financial rows across the full portfolio.

### Short Template (all 9 complete)

| Property | Occupancy | Financial | Notes |
|---|---|---|---|
| 55 Pharr | ✅ | ✅ | full |
| Perry Heights | ✅ | ✅ | full (63 financial records back to Jan 2021) |
| Kensington Place | ✅ | ✅ | full (63 financial records, 36 months collections) |
| Longview | ✅ | ✅ | full (63 financial records, 33 months collections) |
| Abbey Lake | ✅ | ✅ | full |
| Georgetown | ✅ | ✅ | full |
| Portico | ✅ | ✅ | full |
| The Turn | ✅ | ✅ | ~48 weeks occupancy (newer acquisition — as complete as available) |
| Woodland Commons | ✅ | ✅ | ~48 weeks occupancy (newer acquisition — as complete as available) |
| Marbella | ✅ | ✅ | 114 weeks occupancy from Jan 2024 + 27 months financial |

### Long Template (all 10 complete)

| Property | Occupancy | Financial | Notes |
|---|---|---|---|
| 55 Pharr | ✅ | ✅ | full (work orders included) |
| Capella | ✅ | ✅ | work orders not yet backfilled |
| Emerson 1600 | ✅ | ✅ | full (income/expense + cash filled via parser) |
| Marsh Point | ✅ | ✅ | full (income/expense + cash filled via parser) |
| Manchester at Wesleyan | ✅ | ✅ | 162 occ rows + 59 financial rows from INPUT parse |
| Tapestry Park | ✅ | ✅ | 114 occ rows + 42 financial rows |
| Haven | ✅ | ✅ | 208 occ rows + 78 financial rows |
| Hamptons at East Cobb | ✅ | ✅ | 65 occ rows + 24 financial rows |
| Tall Oaks | ✅ | ✅ | 66 occ rows + 25 financial rows |
| Colony Woods | ✅ | ✅ | 122 occ rows + 72 financial rows |

**Backfill method (current):** Run `parse_long_report.py` against the latest weekly report xlsx for each long template property. The parser extracts every historical range on the INPUT sheet (occupancy M-N, turnover P-Q, income/expense S-X, cash Z-AB, collections AD-AG, historical rents AP-AR, work orders AT-AV) and upserts into the CSV database keyed on (property_code, date). Short template properties were backfilled earlier from their Financial sheet historical columns.

**For future updates:** the same parser is the core of the Monday run — it can process either a full weekly report (backfill) or the current week's output (incremental update), using the same upsert logic.

---

## Open Questions

1. **Renewals & move-outs by month** (Long template INPUT rows 31-44, cols C-E) — where does this data come from? Waiting on answer.
2. **ACE formula** — what is the calculation for the ACE score in Luke's summary? Need from Luke.
3. **Status thresholds** — what ACE values map to No Talk / OTV / Meh / Issue?
4. **3-letter property codes** Luke uses in emails (HAH, 5PB, WCN, LMA, KPG, ALT, PHP, GAM, PLG, TAA, MPH, MGS, CAT, TOC, HEC, E1S, HRP, MWM, TPB) — need the full mapping.
5. **Budget Comparison MTD vs PTD** — for the first month of a fiscal period, MTD and PTD are identical. For other months, does PTD mean year-to-date? Currently using MTD only for current month column.
6. **Projected Occupancy file** — is there one file per property or one combined file? The test file had one property in it but filename has no property code.
7. **Capital Improvements** (Long template INPUT row 24) — the Budget Comparison doesn't always have this. Is it a separate line item or sometimes combined with Routine Replacements?

---

## Key People

| Person | Role | Relevant To |
|---|---|---|
| Asset Living | Property management company | Sends Monday Yardi reports (zip) |
| Emily Patton | Arcan | Sends weekly Cash Report email |
| Luke Mills | Asset Director, Arcan | Sends Tuesday summary to exec team |
| Steve O'Brien | Arcan | Reviews reports |
| Shivaani Komanduri | Arcan | Builds/manages this automation |

---

## Technical Notes

### Network Restrictions
Claude's environment can only access: `api.anthropic.com`, `github.com`, `pypi.org`, `npmjs.com`, and package registries. Cannot reach `api.github.com`, `railway.app`, or any other external services.

**Workaround:** Git clone/push to `github.com` works. Database is stored as CSV files in a GitHub repo and accessed via git operations.

### GitHub Access
- Repo: `shivaanik123/arcan-reports-api`
- Branch: `main`
- Data directory: `data/`
- Token: Fine-grained PAT with Contents read/write permission

### Box Access
- Connected via MCP tool
- Can read file content, list folders, search files, upload files

### Microsoft 365 / Outlook
- Listed as connected MCP server but not yet functional
- URL: `https://microsoft365.mcp.claude.com/mcp`
- Once working: search emails, download attachments for Asset Living reports and Emily's cash report

### Chart Handling (openpyxl)
- Charts are preserved from templates but data ranges must be updated to match actual data rows
- Occupancy chart: y-axis 75%-100%, last 56 weeks, series colors: dark blue (occupancy), medium blue (leased), light blue (projection)
- Financial charts: auto-scale y-axis, last 13 months, data labels on rent and collections series
- Collections chart: y-axis 0-100%
