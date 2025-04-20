"""
Script de scraping initial pour Avito Maroc (section voitures d'occasion)
--------------------------------------------------------
Ce script permet d'extraire les annonces de voitures d'occasion sur le site Avito.ma.
Il collecte les informations de base pour chaque annonce sur plusieurs pages:
- Titre de l'annonce
- Prix
- Date de publication
- Année du véhicule
- Type de carburant
- Type de transmission
- Type de vendeur (particulier ou professionnel)
- URL de l'annonce

Fonctionnalités:
- Navigation automatique entre les pages spécifiées par l'utilisateur
- Conversion des dates relatives en dates absolues
- Création d'identifiants uniques pour chaque annonce
- Sauvegarde des données collectées dans un fichier CSV

Utilisation:
1. Exécutez le script
2. Entrez la plage de pages à scraper lorsque demandé
3. Les résultats seront sauvegardés dans ../data/avito/

Dépendances: selenium, webdriver_manager, datetime, re, csv, os, time
"""


# initial_scraper.py
import time
import csv
import os
import re
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def setup_driver():
    """Configure et initialise le driver Selenium."""
    options = Options()
    options.add_argument("--headless")  # Exécuter sans interface graphique
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--log-level=3")  # Réduire les logs inutiles

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def convert_relative_date(relative_date):
    """Convertit une date relative en date exacte."""
    now = datetime.now()

    # Cas "il y a quelques instants" → prendre l'heure actuelle
    if "quelques instants" in relative_date.lower():
        return now.strftime("%Y-%m-%d %H:%M:%S")  # Date et heure actuelles

    # Extraction du nombre (ex: "5" dans "il y a 5 minutes")
    match = re.search(r'(\d+)', relative_date)
    if match:
        num = int(match.group(1))  # Convertir en entier
    else:
        return "Date inconnue"  # Aucun nombre trouvé

    # Gestion des cas spécifiques
    if "minute" in relative_date:
        exact_date = now - timedelta(minutes=num)
        return exact_date.strftime("%Y-%m-%d %H:%M:%S")  # Garde l'heure

    elif "heure" in relative_date:
        exact_date = now - timedelta(hours=num)
        return exact_date.strftime("%Y-%m-%d %H:%M:%S")  # Garde l'heure

    elif "jour" in relative_date:
        exact_date = now - timedelta(days=num)
        return exact_date.strftime("%Y-%m-%d")  # Supprime l'heure

    elif "mois" in relative_date:
        exact_date = now - timedelta(days=30 * num)  # Approximation
        return exact_date.strftime("%Y-%m-%d")  # Supprime l'heure

    elif "an" in relative_date:
        exact_date = now - timedelta(days=365 * num)  # Approximation
        return exact_date.strftime("%Y-%m-%d")  # Supprime l'heure

    else:
        return "Date inconnue"  # Cas non prévu


def create_folder_name(title, idx):
    """Crée un nom de dossier valide pour stocker les images d'une annonce."""
    # Nettoyer le titre pour obtenir un nom de dossier valide
    folder_name = re.sub(r'[^\w\s-]', '', title)  # Supprimer les caractères non alphanumériques
    folder_name = re.sub(r'\s+', '_', folder_name)  # Remplacer les espaces par des underscores
    folder_name = folder_name[:50]  # Limiter la longueur
    
    # Ajouter l'ID pour garantir l'unicité
    folder_name = f"{idx}_{folder_name}"
    
    return folder_name


def scrape_avito(start_page, end_page):
    """Scrape the car listings on Avito for pages within the specified range."""
    base_url = "https://www.avito.ma/fr/maroc/voitures_d_occasion-%C3%A0_vendre"
    driver = setup_driver()
    
    data = []
    listing_id_counter = 1  # Initialize the global ID counter

    # Loop through pages in the specified range
    for page in range(start_page, end_page + 1):
        url = f"{base_url}?o={page}"
        print(f"🔎 Scraping page {page} of {end_page}: {url}")
        
        driver.get(url)
        driver.set_page_load_timeout(180)  # Increase timeout duration

        # Wait for the page to load correctly
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "sc-1nre5ec-1")))
        except Exception as e:
            print(f"❌ Timeout: Impossible de charger la page {page} ({e})")
            continue  # Try next page instead of breaking entirely

        try:
            # Ensure all content is loaded by scrolling to the bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Wait for additional content to load

            # Find the main container
            main_container = driver.find_element(By.CLASS_NAME, "sc-1nre5ec-1")

            # Get all listings on the page
            listings = main_container.find_elements(By.CSS_SELECTOR, "a.sc-1jge648-0.jZXrfL")

            if not listings:
                print(f"❌ Aucune annonce trouvée sur la page {page} ! Passage à la page suivante.")
                continue  # Continue to next page

            print(f"✅ {len(listings)} annonces trouvées sur la page {page} !")

            # Iterate through the listings
            for listing in listings:
                try:
                    # Title
                    title = listing.find_element(By.CSS_SELECTOR, "p.sc-1x0vz2r-0.iHApav").text.strip() if listing.find_elements(By.CSS_SELECTOR, "p.sc-1x0vz2r-0.iHApav") else "N/A"

                    # Price
                    price = listing.find_element(By.CSS_SELECTOR, "p.sc-1x0vz2r-0.dJAfqm").text.strip() if listing.find_elements(By.CSS_SELECTOR, "p.sc-1x0vz2r-0.dJAfqm") else "Prix non spécifié"

                    # Publication date
                    pub_date_raw = listing.find_element(By.CSS_SELECTOR, "p.sc-1x0vz2r-0.layWaX").text.strip() if listing.find_elements(By.CSS_SELECTOR, "p.sc-1x0vz2r-0.layWaX") else "N/A"
                    pub_date = convert_relative_date(pub_date_raw)  # Convert to exact date

                    # Year
                    year = listing.find_element(By.XPATH, ".//span[contains(text(),'20')]").text.strip() if listing.find_elements(By.XPATH, ".//span[contains(text(),'20')]") else "N/A"

                    # Fuel type
                    fuel_type = listing.find_element(By.XPATH, ".//span[contains(text(),'Essence') or contains(text(),'Diesel') or contains(text(),'Hybride') or contains(text(),'Électrique')]").text.strip() if listing.find_elements(By.XPATH, ".//span[contains(text(),'Essence') or contains(text(),'Diesel') or contains(text(),'Hybride') or contains(text(),'Électrique')]") else "N/A"

                    # Transmission
                    transmission = listing.find_element(By.XPATH, ".//span[contains(text(),'Automatique') or contains(text(),'Manuelle')]").text.strip() if listing.find_elements(By.XPATH, ".//span[contains(text(),'Automatique') or contains(text(),'Manuelle')]") else "N/A"

                    # Listing link
                    link = listing.get_attribute("href") if listing.get_attribute("href") else "N/A"

                    # Creator
                    creator = "Particulier"
                    try:
                        creator_element = listing.find_element(By.CSS_SELECTOR, "p.sc-1x0vz2r-0.hNCqYw.sc-1wnmz4-5.dXzQnB")
                        creator = creator_element.text.strip() if creator_element else "Particulier"
                    except:
                        pass  # If no name found, set to "Particulier" by default

                    # Create folder name for this listing
                    folder_name = create_folder_name(title, listing_id_counter)

                    # Save data
                    data.append([listing_id_counter, title, price, pub_date, year, fuel_type, transmission, creator, link, folder_name])

                    listing_id_counter += 1  # Increment the global counter after each listing

                except Exception as e:
                    print(f"⚠️ Erreur avec l'annonce sur la page {page}: {e}")

        except Exception as e:
            print(f"❌ Erreur lors de l'extraction de la page {page}: {e}")

    driver.quit()

    return data


def save_to_csv(data, filename):
    """Sauvegarde les données dans un fichier CSV dans ../data/avito/."""
    output_folder = os.path.join("..", "data", "avito")  # New directory: ../data/avito/
    os.makedirs(output_folder, exist_ok=True)  # Create if not exists
    output_file = os.path.join(output_folder, filename)

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "Titre", "Prix", "Date de publication", "Année", "Type de carburant", "Transmission", "Créateur", "URL de l'annonce", "Dossier d'images"])
        writer.writerows(data)

    print(f"✅ Données sauvegardées dans {output_file}")
    return output_file


def get_page_range():
    """Demander à l'utilisateur de spécifier une plage de pages à scraper."""
    while True:
        try:
            print("\nVeuillez spécifier la plage de pages à scraper:")
            start_page = int(input("Page de début: "))
            end_page = int(input("Page de fin: "))
            
            if start_page <= 0 or end_page <= 0:
                print("⚠️ Les numéros de page doivent être positifs.")
                continue
                
            if start_page > end_page:
                print("⚠️ La page de début doit être inférieure ou égale à la page de fin.")
                continue
                
            return start_page, end_page
        except ValueError:
            print("⚠️ Veuillez entrer des nombres valides.")


def main():
    """Main function to run the initial scraper."""
    print("🚗 Starting Avito car listings scraper - Initial Phase...")
    
    # Get page range from user
    start_page, end_page = get_page_range()
    print(f"\n📋 Scraping car listings from page {start_page} to page {end_page}...")
    
    # Scrape specified pages
    basic_data = scrape_avito(start_page, end_page)
    
    if basic_data is None or len(basic_data) == 0:
        print("❌ No data found. Exiting program.")
        return
    
    # Save basic listings to CSV
    file_suffix = f"p{start_page}-p{end_page}"
    basic_csv = save_to_csv(basic_data, f"avito_listings_{file_suffix}.csv")
    
    print("\n✅ INITIAL SCRAPING COMPLETED SUCCESSFULLY!")
    print(f"Found {len(basic_data)} listings across {end_page - start_page + 1} pages.")
    print(f"Basic listings saved to: {basic_csv}")
    print("Run the detail_scraper.py script next to gather detailed information.")


if __name__ == "__main__":
    main()