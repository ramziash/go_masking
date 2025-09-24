# Parquet Masking Tool - Usage Guide

## Basic Usage
```bash
./test_masking -input_path your_file.parquet
```

## Column Selection

### Mask Single Column (default: column 3)
```bash
./test_masking -columns 3 -input_path data.parquet
```

### Mask Multiple Columns
```bash
./test_masking -columns "1,3,5" -input_path data.parquet
```

### Mask First Column
```bash
./test_masking -columns 0 -input_path data.parquet
```

## Output Control Options

### Quiet Mode (Recommended for Production)
```bash
./test_masking -quiet -input_path data.parquet
```
- ✅ No console output (clean terminal)  
- ✅ All logs go to `app.log` file
- ✅ Only critical errors shown in console

### Verbose Mode (For Debugging)
```bash
./test_masking -verbose -input_path data.parquet
```
- ✅ Debug-level logging
- ✅ Detailed batch processing info
- ✅ Performance metrics in real-time

### JSON Logs (For Log Analysis Tools)
```bash
./test_masking -json -input_path data.parquet
```
- ✅ Structured JSON output
- ✅ Perfect for ELK Stack, Splunk, etc.
- ✅ Machine-parseable format

### Production Mode (Quiet + JSON)
```bash
./test_masking -quiet -json -input_path data.parquet
```
- ✅ Silent operation
- ✅ Structured logs for monitoring
- ✅ Perfect for automated deployments

## Output Files

| File | Content |
|------|---------|
| `app.log` | All application logs |
| `output.csv` | Masked CSV data |

## Log Levels

| Level | When to Use |
|-------|-------------|
| `ERROR` | Critical failures only |
| `INFO` | General progress (default) |
| `DEBUG` | Detailed debugging info |

## Examples

### Single Column Masking
```bash
./test_masking -columns 2 -input_path customer_data.parquet
```

### Multiple Column Masking  
```bash
./test_masking -columns "1,3,7" -input_path sensitive_data.parquet
```

### Large File Processing (Quiet + Multiple Columns)
```bash
./test_masking -quiet -columns "2,4,6" -input_path large_dataset.parquet
# Monitor progress: tail -f app.log
```

### Development/Troubleshooting  
```bash
./test_masking -verbose -columns 5 -input_path sample.parquet
```

### CI/CD Pipeline
```bash
./test_masking -quiet -json -columns "${MASK_COLUMNS}" -input_path "${INPUT_FILE}"
```

## Monitoring Progress in Quiet Mode

Since quiet mode suppresses console output, monitor progress with:

```bash
# Real-time log monitoring
tail -f app.log

# Check processing rate
grep "processing_rate" app.log

# Check for errors
grep "ERROR" app.log

# Get final summary
grep "Processing completed" app.log
```

## Performance Tips

- Use `-quiet` for maximum performance (no console I/O overhead)
- Use `-json` logs for easier parsing by monitoring tools
- Monitor `app.log` file size for very large datasets
- Check processing rate to optimize chunk sizes
