import math
import yfinance as yf
import pandas as pd
import datetime
import numpy as np
import matplotlib
matplotlib.use('Agg')    # ← select non-interactive backend
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import re
import os
import json

# ─── Configuration Constants ────────────────────────────────────────────────────
STRIKE_RANGE        = 20      # how many strikes above/below the current price to show
CONTRACT_SIZE       = 100     # standard option contract multiplier
DEFAULT_SORT_DESC   = True    # default sort direction (True = descending)
CHART_DIR           = "charts"
# ────────────────────────────────────────────────────────────────────────────────

# create the folder if needed
os.makedirs(CHART_DIR, exist_ok=True)

def get_chain_dfs(symbol: str, expiry: str):
    """
    Fetch calls and puts for a given symbol & expiry in one network call.
    Returns two DataFrames (calls, puts) with columns:
      Strike | Last | Change | %Chg | Open Int | IV | Volume
    """
    tk    = yf.Ticker(symbol)
    chain = tk.option_chain(expiry)

    def prep(side_df, side_name):
        # raw columns
        raw_cols = side_df.columns.tolist()

        # desired mapping: upstream → output
        mapping = {
            'strike':            'Strike',
            'lastPrice':         'Last',
            'change':            'Change',
            'percentChange':     '%Chg',
            'openInterest':      'Open Int',
            'impliedVolatility': 'IV',
            'volume':            'Volume'
        }

        # debug: report any missing upstream columns
        missing = [up for up in mapping if up not in raw_cols]
        if missing:
            print(f"Missing columns in {side_name} chain for {symbol} {expiry}: {missing}")
            print("Raw columns available:", raw_cols)

        # build the output DataFrame by safely .get()
        out = pd.DataFrame({ out_name: side_df.get(up_name, np.nan)
                             for up_name, out_name in mapping.items() })

        # sort and reset index
        return out.sort_values('Strike').reset_index(drop=True)

    calls = prep(chain.calls, 'calls')
    puts  = prep(chain.puts,  'puts')

    return calls, puts

def compute_gex(df: pd.DataFrame, S: float, T: float) -> pd.DataFrame:
    """
    Given a DataFrame with 'Strike', 'IV', and 'Open Int',
    compute Black-Scholes gamma and Gamma Exposure (GEX).
    """
    K     = df['Strike'].to_numpy()
    sigma = df['IV'].to_numpy()
    r     = 0.0
    t     = T

    # Black-Scholes d1
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * t) / (sigma * np.sqrt(t))
    pdf = np.exp(-0.5 * d1**2) / np.sqrt(2 * np.pi)

    # Gamma per contract
    gamma = pdf / (S * sigma * np.sqrt(t))

    # Total Gamma Exposure
    gex = gamma * df['Open Int'].to_numpy() * CONTRACT_SIZE

    out = df.copy()
    out['GEX'] = gex
    return out

def calculate_net_gex(
    strikes, ivs, ois, S0, r, q, T
):
    """
    strikes: array of K
    ivs:    array of sigma
    ois:    array of open interest
    S0:     spot price
    r:      risk-free rate
    q:      dividend yield
    T:      time to expiration (years)
    """
    # 1) Forward price
    F = S0 * np.exp((r - q) * T)
    
    # 2) Compute d1 and gamma
    d1 = (np.log(F / strikes) + 0.5 * ivs**2 * T) / (ivs * np.sqrt(T))
    pdf = np.exp(-0.5 * d1**2) / np.sqrt(2 * np.pi)
    gamma = pdf / (F * ivs * np.sqrt(T))
    
    # 3) Per‐strike GEX
    gex = ois * gamma * 100 * F
    
    # 4) Separate calls/puts by sign in ois array (positive=call, negative=put)
    return gex.sum()


def center_strikes(df: pd.DataFrame, current_price: float, radius: int):
    """
    Round price to nearest $0.1, find nearest strike row,
    and return ±radius strikes around it.
    """
    center = round(current_price * 10) / 10.0
    idx    = (df['Strike'] - center).abs().idxmin()
    start  = max(idx - radius, 0)
    end    = min(idx + radius, len(df) - 1)
    return df.iloc[start:end+1].reset_index(drop=True), center

def sort_by_open_interest(df: pd.DataFrame, descending: bool = DEFAULT_SORT_DESC):
    """Sort by Open Interest."""
    return df.sort_values(by='Open Int', ascending=not descending)

def sort_by_volume(df: pd.DataFrame, descending: bool = DEFAULT_SORT_DESC):
    """Sort by Volume."""
    return df.sort_values(by='Volume', ascending=not descending)

def sort_by_strike(df: pd.DataFrame, ascending: bool = True):
    """Sort by Strike price."""
    return df.sort_values(by='Strike', ascending=ascending)

def _slugify(text: str) -> str:
    """Make a filesystem‐safe slug from the title."""
    return re.sub(r'[^A-Za-z0-9]+', '_', text).strip('_')

def plot_heatmap(metrics_df: pd.DataFrame, title: str):
    """
    Render a heatmap with a purple→red scale and black/yellow font contrast.
    • index = ['GEX','Open Int']
    • columns = strike prices
    """
    # prepare figure
    fig, ax = plt.subplots(
        figsize=(len(metrics_df.columns) * 0.5 + 2, 3),
        dpi=100
    )

    # colormap and normalization
    cmap = plt.get_cmap('winter')  
    norm = Normalize(vmin=metrics_df.values.min(),
                     vmax=metrics_df.values.max())

    # draw heatmap
    im = ax.imshow(metrics_df.values, aspect='auto', cmap=cmap, norm=norm)

    # axis labels
    ax.set_yticks([0, 1])
    ax.set_yticklabels(metrics_df.index)
    ax.set_xticks(np.arange(metrics_df.shape[1]))
    ax.set_xticklabels(metrics_df.columns, rotation=90)
    ax.set_title(title)

    # colorbar
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label('Value')

    # annotate with black on light, yellow on dark
    for i in range(metrics_df.shape[0]):
        for j in range(metrics_df.shape[1]):
            val = metrics_df.iat[i, j]
            # normalized value between 0–1
            frac = norm(val)
            color = 'black' if frac > 0.5 else 'white'
            ax.text(j, i, f"{val:.0f}",
                    ha='center', va='center',
                    color=color, fontsize=8)

    plt.tight_layout()
    fn = f"{_slugify(title)}_heatmap.png"
    filepath = os.path.join(CHART_DIR, fn)
    plt.savefig(filepath)
    plt.close()
    print(f"Heatmap saved to {fn}")

def plot_gex_oi_chart(gex_df: pd.DataFrame, title: str):
    """
    Render a grouped bar chart for GEX vs Open Interest across strikes.
    """
    strikes = gex_df['Strike'].to_numpy()
    gex_vals = gex_df['GEX'].to_numpy()
    oi_vals  = gex_df['Open Int'].to_numpy()
    x = np.arange(len(strikes))
    width = 0.35

    fig, ax = plt.subplots(
        figsize=(len(strikes) * 0.4 + 3, 4),
        dpi=100
    )

    ax.bar(x - width/2, gex_vals, width, label='GEX')
    ax.bar(x + width/2, oi_vals, width, label='Open Int')

    ax.set_xticks(x)
    ax.set_xticklabels(strikes, rotation=90)
    ax.set_xlabel('Strike')
    ax.set_ylabel('Value')
    ax.set_title(title)
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.5)

    plt.tight_layout()
    fn = f"{_slugify(title)}_barchart.png"
    filepath = os.path.join(CHART_DIR, fn)
    plt.savefig(filepath)
    plt.close()
    print(f"Chart saved to {fn}")

def plot_net_gex(calls_gex: pd.DataFrame,
                 puts_gex: pd.DataFrame,
                 title: str,
                 symbol: str,
                 expiry: str,
                 current_price: float,
                 chart_dir: str = CHART_DIR):
    """
    Bar chart of net GEX per strike (calls minus puts), centered on zero.
    Positive bars in green, negative in red.
    """
    # align on strikes
    df = pd.merge(
        calls_gex[['Strike','GEX']],
        puts_gex[['Strike','GEX']],
        on='Strike', how='inner', suffixes=('_call','_put')
    )
    df['Net_GEX'] = df['GEX_call'] + df['GEX_put']

    strikes = df['Strike'].to_numpy()
    net_vals = df['Net_GEX'].to_numpy()
    colors   = ['green' if v >= 0 else 'red' for v in net_vals]

    x = np.arange(len(strikes))
    fig, ax = plt.subplots(
        figsize=(len(strikes)*0.3 + 3, 4), dpi=100
    )

    ax.bar(x, net_vals, color=colors)
    ax.axhline(0, color='black', linewidth=1)   # zero line
    ax.set_xticks(x)
    ax.set_xticklabels(strikes, rotation=90)
    ax.set_xlabel('Strike')
    ax.set_ylabel('Net GEX')
    ax.set_title(title)
    ax.grid(True, linestyle='--', alpha=0.3)

    # vertical line at current underlying price
    x_pos = np.interp(current_price, strikes, x)
    ax.axvline(x_pos, color='blue', linestyle='--', linewidth=1.5,
               label=f'Underlying: ${current_price:.2f}')
    
    y_limit = max(abs(net_vals).max(), 1) * 1.1
    ax.set_ylim(-y_limit, y_limit)
    
    plt.tight_layout()
    fname = f"{_slugify(title)}_net_gex.png"
    filepath = os.path.join(chart_dir, fname)
    plt.savefig(filepath)
    plt.close()
    print(f"Net GEX chart saved to {filepath}")

def export_for_ai(df: pd.DataFrame, filename: str):
    """
    Export key fields for AI consumption in JSON, containing:
      • gex_summary: strike, open_interest, gex
      • chain_by_strike: full chain sorted by strike with GEX included
    Writes into CHART_DIR.
    """
    # 1) Sort by strike
    chain_sorted = df.sort_values(by='Strike').reset_index(drop=True)

    # 2) Build GEX summary list
    gex_summary = chain_sorted[['Strike', 'Open Int', 'GEX']].rename(columns={
        'Strike': 'strike',
        'Open Int': 'open_interest',
        'GEX': 'gex'
    }).to_dict(orient='records')

    # 3) Build full chain list
    chain_by_strike = chain_sorted.rename(columns={
        'Strike': 'strike',
        'Last': 'last_price',
        'Change': 'change',
        '%Chg': 'percent_change',
        'Open Int': 'open_interest',
        'IV': 'implied_volatility',
        'Volume': 'volume',
        'GEX': 'gex'
    }).to_dict(orient='records')

    export_obj = {
        'gex_summary': gex_summary,
        'chain_by_strike': chain_by_strike
    }

    # 4) Write JSON into CHART_DIR
    filepath = os.path.join(CHART_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(export_obj, f, indent=2)

    print(f"Exported {len(chain_by_strike)} strikes with GEX to {filepath}")


def main():
    symbol = input("Enter ticker symbol (e.g. AAPL): ").upper()
    tk     = yf.Ticker(symbol)

    # fetch underlying price once
    info  = tk.info
    price = info.get('regularMarketPrice') or info.get('previousClose')
    if price is None:
        price = tk.history(period='1d')['Close'].iloc[-1]

    # list expirations
    expiries = tk.options
    print("\nAvailable expirations:")
    for i, e in enumerate(expiries):
        print(f"  [{i}] {e}")

    idx    = int(input("\nChoose an expiration index: "))
    expiry = expiries[idx]

    # fetch & prepare
    calls_full, puts_full = get_chain_dfs(symbol, expiry)
    calls_slice, center   = center_strikes(calls_full, price, STRIKE_RANGE)
    puts_slice, _         = center_strikes(puts_full, price, STRIKE_RANGE)

    # compute days to expiration
    exp_date = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    now      = datetime.datetime.now()
    T        = max((exp_date - now).days / 365, 1e-4)

    # compute GEX
    r = 0.0          # your risk-free rate
    q = 0.0          # your dividend yield
    # arrays to pass in
    K_calls = calls_slice['Strike'].to_numpy()
    σ_calls = calls_slice['IV'].to_numpy()
    OI_calls = calls_slice['Open Int'].to_numpy()

    K_puts  = puts_slice['Strike'].to_numpy()
    σ_puts  = puts_slice['IV'].to_numpy()
    OI_puts = puts_slice['Open Int'].to_numpy()

    calls_gex = compute_gex(calls_slice, price, T)
    puts_gex  = compute_gex(puts_slice,  price, T)

    net_gex_calls = calculate_net_gex(K_calls, σ_calls, OI_calls, price, r, q, T)
    net_gex_puts  = calculate_net_gex(K_puts,  σ_puts,  OI_puts,  price, r, q, T)

    # output header
    print(f"\nCurrent underlying price: ${price:.2f}")
    print(f"Centering on strike: {center:.1f}")
    print(f"Displaying ±{STRIKE_RANGE} strikes → {2*STRIKE_RANGE+1} rows each\n")
    print(f"Net GEX (Calls): {net_gex_calls:,.0f}")
    print(f"Net GEX (Puts):  {net_gex_puts:,.0f}")
    print(f"Total Net GEX:   {net_gex_calls - net_gex_puts:,.0f}")

    # print tables
    print("=== Calls ===")
    print(calls_slice.to_string(index=False))

    print("\n=== Puts ===")
    print(puts_slice.to_string(index=False))

    

     # render charts for Calls
    metrics_calls = calls_gex[['GEX','Open Int']].T
    metrics_calls.columns = calls_gex['Strike']
    metrics_puts = puts_gex[['GEX','Open Int']].T
    metrics_puts.columns = puts_gex['Strike']
    plot_heatmap(metrics_calls, f"{symbol} {expiry} Calls: GEX & Open Interest")
    plot_heatmap(metrics_puts, f"{symbol} {expiry} Puts: GEX & Open Interest")
    plot_net_gex(
        calls_gex,
        puts_gex,
        f"{symbol} {expiry} Net Gamma Exposure",  # title
        symbol,
        expiry,
        current_price=price,                      # <-- must be the float price
        chart_dir=CHART_DIR
    )
    
    plot_gex_oi_chart(calls_gex,   f"{symbol} {expiry} Calls: GEX vs Open Interest")
    plot_gex_oi_chart(puts_gex,   f"{symbol} {expiry} Puts: GEX vs Open Interest")

    export_for_ai(calls_gex, f'{symbol} {expiry} calls_gex_export.json')
    export_for_ai(puts_gex,  f'{symbol} {expiry} puts_gex_export.json')

    # example sorts
    # print(f"\nTop {STRIKE_RANGE} Calls by Open Interest:")
    # print(sort_by_open_interest(calls_slice).head(STRIKE_RANGE).to_string(index=False))

    # print(f"\nTop {STRIKE_RANGE} Puts by Volume:")
    # print(sort_by_volume(puts_slice).head(STRIKE_RANGE).to_string(index=False))

    # print(f"\nLowest {STRIKE_RANGE} Call Strikes:")
    # print(sort_by_strike(calls_slice).head(STRIKE_RANGE).to_string(index=False))


if __name__ == "__main__":
    main()
