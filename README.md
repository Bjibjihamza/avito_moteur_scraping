# Car Listings Scraper for Avito.ma and Moteur.ma

This project contains Python scripts for scraping car listings from Avito.ma and Moteur.ma. It automates the collection of car-related data (such as price, model, year, fuel type) and downloads the associated images. The collected data is stored in CSV format.

## Features

- **Avito Scraper**: Scrapes car listings from Avito.ma and collects basic car information (title, price, year, fuel type, transmission, etc.) and downloads associated images.
- **Moteur Scraper**: Scrapes car listings from Moteur.ma with similar functionality and saves the data and images in a structured format.

## Requirements

- Python 3.x
- Selenium (for web scraping)
- Requests (for downloading images)
- Webdriver-manager (for managing the ChromeDriver)
- Pandas (for data manipulation and CSV handling)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Bjibjihamza/avito_moteur_scraping.git
   cd avito_moteur_scraping
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### 1. Running the Avito Scraper

To run the scraper for Avito.ma, execute the following command:

```bash
python scraping_select_pages/avito_main.py
```

This will scrape listings from the first page of Avito.ma, extract car details, and download the associated images. The data will be saved to `../data/avito/avito_complete.csv`, and the images will be stored in `../data/avito/images/`.

### 2. Running the Moteur Scraper

To scrape listings from Moteur.ma, execute the following command:

```bash
python scraping_select_pages/moteur_main.py
```

This script will collect similar data from Moteur.ma, store it in `../data/moteur/moteur_complete.csv`, and download images to `../data/moteur/images/`.

## Output

* The CSV files (`avito_complete.csv` and `moteur_complete.csv`) will be stored in `../data/avito/` and `../data/moteur/`, respectively.
* The images will be saved in separate folders for each listing within the `images/` directory.

## Directory Structure

```
avito_moteur_scraping/
├── scraping_select_pages/
│   ├── avito_main.py
│   └── moteur_main.py
├── data/
│   ├── avito/
│   │   ├── avito_complete.csv
│   │   └── images/
│   └── moteur/
│       ├── moteur_complete.csv
│       └── images/
└── requirements.txt
```

## License

[Include your license information here]

## Contributors

[Your name and contact information]
