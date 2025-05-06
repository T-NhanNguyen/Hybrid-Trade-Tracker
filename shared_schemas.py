from pydantic import BaseModel, Field, validator, HttpUrl, ConfigDict
from datetime import date, datetime
from typing import List, Optional, Literal


class StrikeModel(BaseModel):
    strike: float = Field(..., gt=0, description="Strike price, must be positive")
    type: Literal['call', 'put'] = Field(..., description="Option type: 'call' or 'put'")


class RationaleModel(BaseModel):
    support_zones: List[float] = Field(..., description="Key support levels")
    resistance_zones: List[float] = Field(..., description="Key resistance levels")
    rsi: float = Field(..., ge=0, le=100, description="Relative Strength Index (0-100)")
    gamma_magnets: List[float] = Field(..., description="Gamma magnet levels")
    gamma_cliffs: List[float] = Field(..., description="Gamma cliff levels")
    expected_move: float = Field(..., gt=0, description="Expected move magnitude")
    iv_rank: float = Field(..., ge=0, le=100, description="Implied volatility rank (0-100)")
    iv_percentile: Optional[float] = Field(None, ge=0, le=100, description="Implied volatility percentile (0-100)")
    liquidity_notes: str = Field(..., description="Notes on liquidity and open interest")


class TradeInputModel(BaseModel):
    trade_id: str = Field(
        ..., 
        description="Unique trade identifier (trimmed & uppercased)",
        min_length=1
    )
    ticker: str = Field(
        ..., 
        description="Underlying ticker symbol (uppercase)",
        min_length=1
    )
    status: str = Field(
        ..., 
        description="Distinction between a speculation and actual trade",
        min_length=1
    )
    strategy: Literal[
    # Basic
    "long_call",
    "long_put",
    "covered_call",
    "cash_secured_put",
    "naked_call",
    "naked_put",

    # Spreads
    "debit_spread",
    "credit_spread",
    "call_spread",
    "put_spread",
    "poor_mans_covered_call",
    "calendar_spread",
    "ratio_back_spread",

    # Advanced
    "iron_condor",
    "butterfly",
    "collar",
    "diagonal_spread",
    "double_diagonal",
    "straddle",
    "strangle",
    "covered_strangle",
    "synthetic_put",
    "reverse_conversion",

    # Custom (by leg-count)
    "custom_2_legs",
    "custom_3_legs",
    "custom_4_legs",
    "custom_5_legs",
    "custom_6_legs",
    "custom_8_legs",
    ]= Field(
        ..., 
        description="Trading strategy type"
    )
    expiration_date: date = Field(
        ..., 
        description="Options expiration date (YYYY-MM-DD)"
    )
    strikes: List[StrikeModel] = Field(..., description="List of strikes involved in the trade")
    direction: Literal['bullish', 'neutral', 'bearish'] = Field(
        ..., 
        description="Directional bias of the trade"
    )
    rationale: RationaleModel = Field(..., description="Quantitative and qualitative rationale for the trade")
    max_pain_price: float = Field(..., ge=0, description="Max pain price level for the underlying")
    news_links: List[HttpUrl] = Field(..., description="Related news or research URLs")
    entry_price: Optional[float] = Field(
        default=None,
        gt=0,
        description="Optional entry price for the trade"
    )
    exit_price: Optional[float] = Field(
        default=None,
        gt=0,
        description="Optional exit price for the trade"
    )
    timestamp: datetime = Field(
        ..., 
        description="ISO-8601 timestamp when the trade spec was generated"
    )

    @validator('trade_id', pre=True)
    def normalize_trade_id(cls, v: str) -> str:
        return v.strip().upper()

    @validator('ticker', pre=True)
    def normalize_ticker(cls, v: str) -> str:
        return v.strip().upper()

    @validator('expiration_date')
    def check_expiration_future(cls, v: date) -> date:
        if v < date.today():
            raise ValueError('expiration_date must be today or in the future')
        return v

    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v: str) -> datetime:
        return v

    model_config = ConfigDict(
      json_encoders = {
        date:     lambda d: d.isoformat(),
        datetime: lambda dt: dt.isoformat(),
      }
    )
