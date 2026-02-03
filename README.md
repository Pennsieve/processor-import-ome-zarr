# processor-import-ome-zarr

Pennsieve processor for importing zipped OME-Zarr directories as viewer assets.

## Overview

This processor extracts zipped OME-Zarr archives and imports them to Pennsieve as viewer assets. It uses the import service's `asset_name` option to create a single viewer asset record pointing to the shared prefix, rather than individual records per file.

## Features

- Extracts zipped OME-Zarr archives
- Validates OME-Zarr directory structure
- Creates import manifests with batching for large file sets
- Parallel file uploads to S3
- Automatic session refresh on authentication failures

## Project Structure

```
processor-import-ome-zarr/
├── processor/                    # Main application code
│   ├── main.py                  # Entry point
│   ├── config.py                # Configuration management
│   ├── extractor.py             # ZIP extraction and OME-Zarr handling
│   ├── importer.py              # Pennsieve import orchestration
│   ├── utils.py                 # Utility functions
│   └── clients/                 # API clients
│       ├── base_client.py       # Base client with retry logic
│       ├── authentication_client.py  # AWS Cognito auth
│       ├── import_client.py     # Import manifest API
│       └── workflow_client.py   # Workflow management API
├── tests/                       # Test suite
├── Dockerfile                   # Docker configuration
├── docker-compose.yml           # Docker Compose configuration
└── dev.env                      # Development environment variables
```

## Configuration

Configuration is managed through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `ENVIRONMENT` | Runtime environment (local/production) | `local` |
| `INPUT_DIR` | Directory containing input ZIP files | Required |
| `OUTPUT_DIR` | Directory for extracted files | Required |
| `PENNSIEVE_API_HOST` | Pennsieve API host | `https://api.pennsieve.net` |
| `PENNSIEVE_API_HOST2` | Pennsieve import service host | `https://api2.pennsieve.net` |
| `PENNSIEVE_API_KEY` | Pennsieve API key | Required for import |
| `PENNSIEVE_API_SECRET` | Pennsieve API secret | Required for import |
| `INTEGRATION_ID` | Workflow integration UUID | Required for import |
| `IMPORTER_ENABLED` | Enable Pennsieve upload | `false` (local), `true` (other) |
| `ASSET_TYPE` | Viewer asset type | `ome-zarr` |

## Development

### Setup

```bash
# Create virtual environment
make venv
source venv/bin/activate

# Install dependencies
make install

# Install pre-commit hooks
make pre-commit
```

### Running Tests

```bash
# Run tests
make test

# Run tests with coverage
make test-cov
```

### Linting

```bash
make lint
```

### Running Locally

1. Copy a zipped OME-Zarr file to `data/input/`
2. Run with Docker:

```bash
make run
```

## Usage

The processor expects:

1. A single ZIP file in the input directory
2. The ZIP file contains an OME-Zarr directory (identified by `.zattrs` or `.zgroup` files)

The processor will:

1. Extract the ZIP file
2. Locate the OME-Zarr root directory
3. Collect all files within the OME-Zarr directory
4. Create an import manifest with `asset_name` set to the OME-Zarr directory name
5. Upload all files to S3 via presigned URLs

The `asset_name` option signals to the import service that all files should be grouped under a single viewer asset record, with the top-level prefix becoming the viewer asset path.

## License

See [LICENSE](LICENSE) for details.
