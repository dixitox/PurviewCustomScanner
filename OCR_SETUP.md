# OCR Setup Instructions

The scanner now includes OCR (Optical Character Recognition) support to extract text from scanned PDFs and image-based documents. This helps reduce "Empty Content" classifications.

## Prerequisites

### 1. Install Tesseract OCR

Tesseract is a system-level OCR engine that must be installed separately:

#### Windows:
1. Download the installer from: https://github.com/UB-Mannheim/tesseract/wiki
2. Run the installer (recommended: use default path `C:\Program Files\Tesseract-OCR`)
3. Add Tesseract to your PATH, or set the path in Python:
   ```python
   import pytesseract
   pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
   ```

#### macOS:
```bash
brew install tesseract
```

#### Linux (Ubuntu/Debian):
```bash
sudo apt-get update
sudo apt-get install tesseract-ocr
sudo apt-get install poppler-utils  # Required for pdf2image
```

### 2. Install Python Packages

The required Python packages are in `requirements.txt`:
```bash
pip install -r requirements.txt
```

This includes:
- `pytesseract`: Python wrapper for Tesseract
- `pdf2image`: Converts PDF pages to images
- `Pillow`: Image processing library

## How It Works

1. **Standard extraction first**: The scanner attempts normal text extraction from PDFs using PyPDF2
2. **OCR fallback**: If less than 50 characters are extracted, it automatically tries OCR:
   - Converts PDF pages to images (up to 10 pages by default)
   - Uses Tesseract to extract text from images
   - Returns the better result

3. **Graceful degradation**: If OCR libraries aren't installed, the scanner continues working but logs a warning

## Performance Notes

- OCR processing is slower than standard text extraction (2-5 seconds per page)
- By default, only the first 10 pages are processed via OCR to balance accuracy and performance
- OCR works best with clear, high-resolution scans

## Verification

After installation, you can test OCR is working:

```python
from pdf2image import convert_from_path
import pytesseract

# Should return version string if properly installed
print(pytesseract.get_tesseract_version())
```

## Troubleshooting

**Error: "pytesseract.pytesseract.TesseractNotFoundError"**
- Tesseract OCR is not installed or not in PATH
- Set the path manually (see Windows instructions above)

**Error: "Unable to get page count. Is poppler installed?"**
- On Windows: Install poppler (download from https://github.com/oschwartz10612/poppler-windows/releases/)
- On Linux: `sudo apt-get install poppler-utils`
- On macOS: `brew install poppler`

**Poor OCR quality**
- Increase DPI in `extractPDFContentWithOCR()` (default: 200, try 300)
- Ensure original PDF scans are high quality
- Consider pre-processing images (deskewing, denoising)
