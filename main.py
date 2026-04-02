"""
Arcan Weekly Reports API
Deploy on Railway alongside your Postgres database.
Provides read/write access for the weekly report generator.
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
import os
import psycopg2
import psycopg2.extras

app = FastAPI(title="Arcan Reports API")

# ── Config ───────────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:ysBxQxKGOxlvIFfhCVrEYlApmrqElAMB@shinkansen.proxy.rlwy.net:55881/railway")
API_KEY = os.environ.get("API_KEY", "arcan-weekly-reports-2026")


# ── Auth ─────────────────────────────────────────────────────────────────────
def verify_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ── DB Connection ────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL)


def query(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            if cur.description:
                return cur.fetchall()
            conn.commit()
            return []
    finally:
        conn.close()


def execute(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
    finally:
        conn.close()


# ── Models ───────────────────────────────────────────────────────────────────
class WeeklyDataIn(BaseModel):
    property_code: str
    week_date: date
    new_leads: int = 0
    tours: int = 0
    applications: int = 0
    move_ins: int = 0
    move_outs: int = 0
    make_ready_count: int = 0
    work_orders_count: int = 0
    occupancy_pct: Optional[float] = None
    leased_pct: Optional[float] = None
    units: Optional[int] = None
    occupied: Optional[int] = None
    vacant: Optional[int] = None
    notice: Optional[int] = None
    rented: Optional[int] = None
    available: Optional[int] = None
    evictions: Optional[int] = None
    evictions_filed: Optional[int] = None
    evictions_not_filed: Optional[int] = None
    delinquency_0_30: Optional[float] = None
    delinquency_31_60: Optional[float] = None
    delinquency_61_90: Optional[float] = None
    delinquency_over_90: Optional[float] = None
    prepayment: Optional[float] = None
    delinquency_balance: Optional[float] = None


class OccupancyIn(BaseModel):
    property_code: str
    date: date
    occupancy: Optional[float] = None
    leased: Optional[float] = None
    projection: Optional[float] = None
    make_ready: Optional[int] = None
    work_orders: Optional[int] = None
    turnover: Optional[float] = None


class FinancialIn(BaseModel):
    property_code: str
    date: date
    market_rent: Optional[float] = None
    occupied_rent: Optional[float] = None
    revenue: Optional[float] = None
    expenses: Optional[float] = None
    owed: Optional[float] = None
    charges: Optional[float] = None
    collections: Optional[float] = None
    collections_pct: Optional[float] = None
    income_actual: Optional[float] = None
    income_budget: Optional[float] = None
    expense_actual: Optional[float] = None
    expense_budget: Optional[float] = None
    actual_cash: Optional[float] = None
    adjusted_cash: Optional[float] = None
    collected: Optional[float] = None


# ── READ Endpoints ───────────────────────────────────────────────────────────

@app.get("/properties", dependencies=[Depends(verify_key)])
def get_properties():
    return query("SELECT * FROM properties ORDER BY property_code")


@app.get("/properties/{code}", dependencies=[Depends(verify_key)])
def get_property(code: str):
    rows = query("SELECT * FROM properties WHERE property_code = %s", (code,))
    if not rows:
        raise HTTPException(404, "Property not found")
    return rows[0]


@app.get("/historical_occupancy/{code}", dependencies=[Depends(verify_key)])
def get_historical_occupancy(code: str):
    return query(
        "SELECT * FROM historical_occupancy WHERE property_code = %s ORDER BY date",
        (code,)
    )


@app.get("/historical_financial/{code}", dependencies=[Depends(verify_key)])
def get_historical_financial(code: str):
    return query(
        "SELECT * FROM historical_financial WHERE property_code = %s ORDER BY date",
        (code,)
    )


@app.get("/weekly_data/{code}", dependencies=[Depends(verify_key)])
def get_weekly_data(code: str, week_date: Optional[date] = None):
    if week_date:
        return query(
            "SELECT * FROM weekly_data WHERE property_code = %s AND week_date = %s",
            (code, week_date)
        )
    return query(
        "SELECT * FROM weekly_data WHERE property_code = %s ORDER BY week_date",
        (code,)
    )


@app.get("/monthly_data/{code}", dependencies=[Depends(verify_key)])
def get_monthly_data(code: str, year: Optional[int] = None, month: Optional[int] = None):
    if year and month:
        return query(
            "SELECT * FROM monthly_data WHERE property_code = %s AND EXTRACT(YEAR FROM month_date) = %s AND EXTRACT(MONTH FROM month_date) = %s",
            (code, year, month)
        )
    return query(
        "SELECT * FROM monthly_data WHERE property_code = %s ORDER BY month_date",
        (code,)
    )


# ── WRITE Endpoints ──────────────────────────────────────────────────────────

@app.post("/weekly_data", dependencies=[Depends(verify_key)])
def upsert_weekly_data(data: WeeklyDataIn):
    existing = query(
        "SELECT id FROM weekly_data WHERE property_code = %s AND week_date = %s",
        (data.property_code, data.week_date)
    )
    if existing:
        execute("""
            UPDATE weekly_data SET
                new_leads=%s, tours=%s, applications=%s, move_ins=%s, move_outs=%s,
                make_ready_count=%s, work_orders_count=%s, occupancy_pct=%s, leased_pct=%s,
                units=%s, occupied=%s, vacant=%s, notice=%s, rented=%s, available=%s,
                evictions=%s, evictions_filed=%s, evictions_not_filed=%s,
                delinquency_0_30=%s, delinquency_31_60=%s, delinquency_61_90=%s,
                delinquency_over_90=%s, prepayment=%s, delinquency_balance=%s
            WHERE property_code=%s AND week_date=%s
        """, (
            data.new_leads, data.tours, data.applications, data.move_ins, data.move_outs,
            data.make_ready_count, data.work_orders_count, data.occupancy_pct, data.leased_pct,
            data.units, data.occupied, data.vacant, data.notice, data.rented, data.available,
            data.evictions, data.evictions_filed, data.evictions_not_filed,
            data.delinquency_0_30, data.delinquency_31_60, data.delinquency_61_90,
            data.delinquency_over_90, data.prepayment, data.delinquency_balance,
            data.property_code, data.week_date
        ))
        return {"status": "updated"}
    else:
        execute("""
            INSERT INTO weekly_data (
                property_code, week_date, new_leads, tours, applications, move_ins, move_outs,
                make_ready_count, work_orders_count, occupancy_pct, leased_pct,
                units, occupied, vacant, notice, rented, available,
                evictions, evictions_filed, evictions_not_filed,
                delinquency_0_30, delinquency_31_60, delinquency_61_90,
                delinquency_over_90, prepayment, delinquency_balance
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            data.property_code, data.week_date,
            data.new_leads, data.tours, data.applications, data.move_ins, data.move_outs,
            data.make_ready_count, data.work_orders_count, data.occupancy_pct, data.leased_pct,
            data.units, data.occupied, data.vacant, data.notice, data.rented, data.available,
            data.evictions, data.evictions_filed, data.evictions_not_filed,
            data.delinquency_0_30, data.delinquency_31_60, data.delinquency_61_90,
            data.delinquency_over_90, data.prepayment, data.delinquency_balance
        ))
        return {"status": "created"}


@app.post("/historical_occupancy", dependencies=[Depends(verify_key)])
def upsert_occupancy(data: OccupancyIn):
    existing = query(
        "SELECT id FROM historical_occupancy WHERE property_code = %s AND date = %s",
        (data.property_code, data.date)
    )
    if existing:
        execute("""
            UPDATE historical_occupancy SET
                occupancy=%s, leased=%s, projection=%s, make_ready=%s, work_orders=%s, turnover=%s
            WHERE property_code=%s AND date=%s
        """, (data.occupancy, data.leased, data.projection, data.make_ready, data.work_orders,
              data.turnover, data.property_code, data.date))
        return {"status": "updated"}
    else:
        execute("""
            INSERT INTO historical_occupancy (property_code, date, occupancy, leased, projection, make_ready, work_orders, turnover)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data.property_code, data.date, data.occupancy, data.leased, data.projection,
              data.make_ready, data.work_orders, data.turnover))
        return {"status": "created"}


@app.post("/historical_financial", dependencies=[Depends(verify_key)])
def upsert_financial(data: FinancialIn):
    existing = query(
        "SELECT id FROM historical_financial WHERE property_code = %s AND date = %s",
        (data.property_code, data.date)
    )
    if existing:
        execute("""
            UPDATE historical_financial SET
                market_rent=%s, occupied_rent=%s, revenue=%s, expenses=%s, owed=%s, charges=%s,
                collections=%s, collections_pct=%s, income_actual=%s, income_budget=%s,
                expense_actual=%s, expense_budget=%s, actual_cash=%s, adjusted_cash=%s, collected=%s
            WHERE property_code=%s AND date=%s
        """, (data.market_rent, data.occupied_rent, data.revenue, data.expenses, data.owed,
              data.charges, data.collections, data.collections_pct, data.income_actual,
              data.income_budget, data.expense_actual, data.expense_budget, data.actual_cash,
              data.adjusted_cash, data.collected, data.property_code, data.date))
        return {"status": "updated"}
    else:
        execute("""
            INSERT INTO historical_financial (
                property_code, date, market_rent, occupied_rent, revenue, expenses, owed, charges,
                collections, collections_pct, income_actual, income_budget, expense_actual,
                expense_budget, actual_cash, adjusted_cash, collected
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data.property_code, data.date, data.market_rent, data.occupied_rent, data.revenue,
              data.expenses, data.owed, data.charges, data.collections, data.collections_pct,
              data.income_actual, data.income_budget, data.expense_actual, data.expense_budget,
              data.actual_cash, data.adjusted_cash, data.collected))
        return {"status": "created"}


@app.get("/health")
def health():
    try:
        query("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}
