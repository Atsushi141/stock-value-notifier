# TSE Integration Test Report
==================================================

**Overall Result**: ✅ PASSED
**Tests Passed**: 7/7
**Success Rate**: 100.0%
**Total Time**: 0.23 seconds

## Data File Exists
**Status**: ✅ PASSED

## Data Loading
**Status**: ✅ PASSED
**Statistics**:
- total_records: 4437
- load_time: 0.045320987701416016
- columns: ['日付', 'コード', '銘柄名', '市場・商品区分', '33業種コード', '33業種区分', '17業種コード', '17業種区分', '規模コード', '規模区分']
- missing_columns: []

## Etf Exclusion
**Status**: ✅ PASSED
**Statistics**:
- initial_count: 4437
- tradable_count: 4108
- final_count: 3706
- excluded_count: 402
- exclusion_in_expected_range: True
- remaining_etfs: 0
- remaining_reits: 0
- excluded_breakdown:
  - ETF・ETN: 335
  - REIT・ベンチャーファンド・カントリーファンド・インフラファンド: 62
  - 出資証券: 2
  - スタンダード（内国株式）: 1
  - プライム（内国株式）: 1
  - グロース（内国株式）: 1

## Regular Stock Extraction
**Status**: ✅ PASSED
**Statistics**:
- stock_count: 3701
- count_in_expected_range: True
- all_have_suffix: True
- sample_stocks: ['1301.T', '1332.T', '1333.T', '1375.T', '1376.T', '1377.T', '1379.T', '1380.T', '1381.T', '1382.T']
- processing_stats:
  - total_records: 4437
  - tradable_stocks: 4108
  - final_stocks: 3701
  - excluded_investment_products: 407
  - market_category_breakdown: {'プライム（内国株式）': 1586, 'スタンダード（内国株式）': 1537, 'グロース（内国株式）': 506, 'PRO Market': 72}
  - sector_17_breakdown: {'情報通信・サービスその他': 1196, '小売': 326, '商社・卸売': 293, '建設・資材': 280, '電機・精密': 278, '素材・化学': 274, '機械': 215, '不動産': 139, '食品': 133, '運輸・物流': 106, '自動車・輸送機': 104, '金融（除く銀行）': 89, '銀行': 79, '医薬品': 74, '鉄鋼・非鉄': 72, '電力・ガス': 27, 'エネルギー資源': 16}
  - size_category_breakdown: {'-': 2050, 'TOPIX Small 2': 662, 'TOPIX Small 1': 494, 'TOPIX Mid400': 395, 'TOPIX Large70': 69, 'TOPIX Core30': 31}
  - processing_timestamp: 2026-01-09T16:09:41.110410

## Fallback Functionality
**Status**: ✅ PASSED
**Statistics**:
- fallback_count: 9000
- all_have_suffix: True
- fallback_mechanism_works: True
- fallback_disabled_works: True
- sample_fallback_stocks: ['1000.T', '1001.T', '1002.T', '1003.T', '1004.T', '1005.T', '1006.T', '1007.T', '1008.T', '1009.T']

## Data Fetcher Integration
**Status**: ✅ PASSED
**Statistics**:
- tse_stock_count: 3701
- fetch_time: 0.06052994728088379
- stocks_match_direct: True
- curated_count: 128
- all_count: 523
- metadata_available: True

## Data Integrity
**Status**: ✅ PASSED
**Statistics**:
- is_valid: True
- total_records: 4437
- issues_count: 0
- issues: []
- distribution_available: True
