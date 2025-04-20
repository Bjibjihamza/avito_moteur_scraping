
# **Car Listings Scraper for Avito and Moteur**

This project consists of two Python scripts that automate the extraction of car listings from the websites `Avito.ma` and `Moteur.ma`, focusing on collecting relevant data and storing it in CSV files while also handling image downloads. The scraped data is sent to Kafka for real-time processing, creating a robust pipeline for further analysis and integration.

---

## **1. Project Overview**

### **1.1 Features**

- **Avito Scraper**:
    
    - Scrapes car listings from the website `Avito.ma`.
        
    - Extracts essential details like title, price, year, fuel type, transmission, and more.
        
    - Downloads images associated with each listing.
        
    - Saves the data in a CSV file.
        
    - Sends the data to a Kafka topic for real-time processing.
        
- **Moteur Scraper**:
    
    - Scrapes car listings from the website `Moteur.ma`.
        
    - Collects basic car information, including title, price, year, fuel type, and seller details.
        
    - Downloads images for each listing.
        
    - Stores the data in CSV format.
        
    - Sends the data to a Kafka topic.
        
- **Output**:
    
    - Both scrapers save data in `../data/avito/` and `../data/moteur/` directories.
        
    - Images are stored in a folder specific to each listing, ensuring better organization.
        

---

## **2. Architecture**

### **2.1 High-Level Overview**

1. **Data Extraction**:
    
    - **Step 1**: The scraper accesses the homepage of `Avito.ma` or `Moteur.ma` and navigates through car listings.
        
    - **Step 2**: For each car listing, it navigates to the detail page and extracts further information like car specifications, seller data, and images.
        
2. **Data Transformation**:
    
    - The scrapers transform relative publication dates into absolute dates.
        
    - The data is cleaned and structured to ensure uniformity in the CSV output.
        
3. **Data Storage**:
    
    - The collected data is stored in structured CSV files for both websites (`avito_complete.csv` and `moteur_complete.csv`).
        
    - Images are saved under separate folders, with the folder names based on each listing's unique identifier.
        
4. **Kafka Integration**:
    
    - After the data extraction and transformation process, each piece of data is sent to a Kafka topic for real-time processing and analysis.
        

---

## **3. Project Setup**

### **3.1 Prerequisites**

The project requires the following Python packages:

- `selenium`: For web scraping and automating browser interactions.
    
- `requests`: For downloading images.
    
- `webdriver-manager`: Manages the appropriate browser driver for Selenium.
    
- `pandas`: For storing and manipulating scraped data in CSV format.
    
- `kafka-python`: For sending data to Kafka.
    

### **3.2 Installation Steps**

1. Clone the repository:
    
    bash
    
    Copy
    
    `git clone https://github.com/yourusername/car-listings-scraper.git cd car-listings-scraper`
    
2. Install the required dependencies:
    
    bash
    
    Copy
    
    `pip install -r requirements.txt`
    

---

## **4. Running the Scrapers**

### **4.1 Avito Scraper**

To run the scraper for `Avito.ma`, execute the following command:

bash

Copy

`python avito_scraper.py`

This will start the scraping process for `Avito.ma`, collect the listings from the homepage, extract detailed data from each listing, and download associated images. The data will be saved in `../data/avito/avito_complete.csv`, and images will be stored in `../data/avito/images/`.

### **4.2 Moteur Scraper**

To scrape listings from `Moteur.ma`, use the following command:

bash

Copy

`python moteur_scraper.py`

The `Moteur` scraper will follow the same logic as the `Avito` scraper, storing the data in `../data/moteur/moteur_complete.csv` and images in `../data/moteur/images/`.