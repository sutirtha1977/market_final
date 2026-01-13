# import pandas as pd
# import requests
# from io import StringIO
# from pathlib import Path
# from nse_constants import US_STOCKS,SNP_500,NASDAQ_100


# def get_html(url):
#     headers = {"User-Agent": "Mozilla/5.0"}
#     r = requests.get(url, headers=headers, timeout=30)
#     r.raise_for_status()
#     return r.text


# # -------------------------------------------------
# # S&P 500
# # -------------------------------------------------
# def download_sp500():
#     print("🔽 Downloading S&P 500 list...")

#     df = pd.read_csv(SNP_500)

#     name_col = "Name" if "Name" in df.columns else "Security"

#     df = df.rename(columns={
#         "Symbol": "symbol",
#         name_col: "stock_name"
#     })[["symbol", "stock_name"]]

#     df["index"] = "S&P 500"
#     return df


# # -------------------------------------------------
# # NASDAQ-100 (Wikipedia safe)
# # -------------------------------------------------
# def download_nasdaq100():
#     print("🔽 Downloading NASDAQ-100 list...")

#     html = get_html(NASDAQ_100)

#     tables = pd.read_html(StringIO(html))

#     target = None
#     for t in tables:
#         cols = [str(c).lower() for c in t.columns]   # ✅ FIX HERE
#         if "ticker" in cols or "symbol" in cols:
#             target = t
#             break

#     if target is None:
#         raise Exception("❌ Could not locate NASDAQ-100 table")

#     df = target.copy()

#     sym_col  = "Ticker" if "Ticker" in df.columns else "Symbol"
#     name_col = "Company" if "Company" in df.columns else "Name"

#     df = df.rename(columns={
#         sym_col: "symbol",
#         name_col: "stock_name"
#     })[["symbol", "stock_name"]]

#     df["index"] = "NASDAQ 100"
#     return df


# # -------------------------------------------------
# # MAIN
# # -------------------------------------------------
# def download_and_merge_us_indices():

#     nasdaq_df = download_nasdaq100()
#     sp500_df  = download_sp500()

#     final_df = pd.concat([nasdaq_df, sp500_df], ignore_index=True)
#     final_df = final_df.drop_duplicates(subset=["symbol"], keep="first")

#     US_STOCKS.parent.mkdir(parents=True, exist_ok=True)
#     final_df.to_csv(US_STOCKS, index=False)

#     print("\n✅ US STOCK UNIVERSE CREATED")
#     print(f"📄 File saved to: {US_STOCKS}")
#     print(f"📊 Total symbols: {len(final_df)}")
#     print("\nPreview:")
#     print(final_df.head(10))


# if __name__ == "__main__":
#     download_and_merge_us_indices()