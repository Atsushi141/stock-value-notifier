# TSE Performance Test Report
==================================================

## System Information
- CPU Count: 8
- Memory Total: 24.0 GB
- Python Version: 3.12.7 (main, Oct 20 2024, 21:43:29) [Clang 16.0.0 (clang-1600.0.26.3)]

## Performance Summary
Performance Summary:
  TSE Data Loading: 0.530s for 4,437 records
  Cached Loading: 0.000s (1570.2x faster)
  Complete Pipeline: 0.050s (4,437 → 3,706 stocks)
  Processing Rate: 88852 records/second
  Peak Memory Usage: +0.1 MB
  Memory Leak: None
  TSE vs Fallback: 2.8x time, 3.5x memory, 0.4x accuracy

## Tse Loading Performance
### Cold Load
- time: 0.530s
- records: 4437
- memory_peak_mb: 4.81 MB
- memory_increase_mb: 12.78 MB
### Warm Load
- time: 0.000s
- records: 4437
- speed_improvement: 1570.1940719830627
- memory_peak_mb: 0.35 MB
### Consistency
- average_time: 0.523s
- min_time: 0.518s
- max_time: 0.525s
- std_dev: 0.0027786613711258614
- coefficient_of_variation: 0.005315395732705363

## Filtering Performance
### Tradable Filtering
- time: 0.013s
- input_records: 4437
- output_records: 4108
- filtering_rate: 341232
- memory_peak_mb: 0.95 MB
### Investment Exclusion
- time: 0.017s
- input_records: 4108
- output_records: 3706
- excluded_records: 402
- processing_rate: 236474
- memory_peak_mb: 1.14 MB
### Complete Pipeline
- time: 0.050s
- input_records: 4437
- output_stocks: 3706
- overall_rate: 88852
- memory_peak_mb: 2.04 MB

## Tse Vs Fallback
### Tse Official
- time: 0.032s
- stock_count: 3701
- memory_peak_mb: 1.70 MB
- memory_increase_mb: 0.02 MB
### Fallback
- time: 0.011s
- stock_count: 9000
- memory_peak_mb: 0.48 MB
- memory_increase_mb: 0.00 MB
### Comparison
- time_ratio: 2.769s
- memory_ratio: 3.521219850203532
- accuracy_ratio: 0.4112222222222222
- tse_reasonably_fast: False
- tse_reasonable_memory: False
### Datafetcher Modes
- tse_official: {'time': 0.608914852142334, 'stock_count': 3701, 'memory_peak_mb': 6.133087158203125}
- all_fallback: {'time': 0.0004112720489501953, 'stock_count': 523, 'memory_peak_mb': 0.044891357421875}
- curated: {'time': 0.00028395652770996094, 'stock_count': 128, 'memory_peak_mb': 0.019797325134277344}

## Memory Patterns
### Repeated Loading
- baseline_mb: 118.59 MB
- average_mb: 118.66 MB
- min_mb: 118.62 MB
- max_mb: 118.72 MB
- memory_growth_mb: 0.09 MB
- peak_increase_mb: 0.12 MB
- has_memory_leak: False
- memory_samples: [118.625, 118.640625, 118.640625, 118.640625, 118.640625, 118.640625, 118.65625, 118.6875, 118.71875, 118.71875]
### Cache Efficiency
- first_load_mb: 118.72 MB
- cached_load_mb: 118.72 MB
- cache_overhead_mb: 0.00 MB

## Scalability
### Scalability Test
- full_dataset_size: 4437
- test_results: [{'input_size': 443, 'percentage': 0.09984223574487266, 'processing_time': 0.0023050308227539062, 'records_per_second': 192188.31940422012, 'output_stocks': 165}, {'input_size': 1109, 'percentage': 0.2499436556231688, 'processing_time': 0.0031239986419677734, 'records_per_second': 354993.7522704724, 'output_stocks': 600}, {'input_size': 2218, 'percentage': 0.4998873112463376, 'processing_time': 0.005048036575317383, 'records_per_second': 439378.74991734757, 'output_stocks': 1524}, {'input_size': 3327, 'percentage': 0.7498309668695065, 'processing_time': 0.006586313247680664, 'records_per_second': 505138.4401085973, 'output_stocks': 2631}, {'input_size': 4437, 'percentage': 1.0, 'processing_time': 0.008812904357910156, 'records_per_second': 503466.26036143274, 'output_stocks': 3706}]
- size_time_correlation: 0.998s
- scalability_rating: excellent
