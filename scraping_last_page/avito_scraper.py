"""
Scraper complet pour le site avito.ma - Extraction et traitement de données automobiles

Ce script automatise la collecte de données d'annonces de voitures d'occasion sur avito.ma
et les transmet à Kafka pour un traitement en temps réel.

Fonctionnalités principales:
- Extraction des annonces automobiles de la page d'accueil du site
- Récupération des informations détaillées pour chaque annonce (caractéristiques du véhicule)
- Téléchargement et stockage des images associées à chaque annonce
- Transformation des dates relatives en dates exactes
- Stockage des données dans un fichier CSV structuré
- Envoi des données vers un topic Kafka pour intégration dans un pipeline de données

Architecture:
1. Extraction des annonces basiques de la première page
2. Visite des pages de détail pour chaque annonce
3. Récupération des informations supplémentaires et téléchargement des images
4. Stockage des données dans un CSV et envoi vers Kafka

Sorties:
- CSV: ../data/avito/avito_complete.csv
- Images: ../data/avito/images/[dossiers_par_annonce]
"""


import time
import csv
import os
import re
import json
import requests
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager




def send_to_kafka(producer, topic, data, headers):
    """Envoie les données au topic Kafka spécifié."""
    if not producer:
        print("❌ Producteur Kafka non disponible")
        return False
    
    try:
        # Convertir les données en dictionnaire pour une meilleure lisibilité
        car_dict = dict(zip(headers, data))
        
        # Envoyer au topic Kafka
        future = producer.send(topic, value=car_dict)
        # Attendre le résultat pour confirmer l'envoi
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Message envoyé au topic '{topic}' partition {record_metadata.partition} offset {record_metadata.offset}")
        return True
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi au topic '{topic}': {e}")
        return False


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


def download_image(image_url, folder_path, image_name):
    """Télécharge une image et la sauvegarde dans le dossier spécifié."""
    try:
        # Ajouter des en-têtes pour simuler un navigateur
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(image_url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()
        
        # Déterminer l'extension de fichier basée sur le Content-Type
        content_type = response.headers.get('Content-Type', '')
        extension = '.jpg'  # Par défaut
        if 'png' in content_type:
            extension = '.png'
        elif 'jpeg' in content_type or 'jpg' in content_type:
            extension = '.jpg'
        
        image_path = os.path.join(folder_path, f"{image_name}{extension}")
        
        with open(image_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        return os.path.basename(image_path)  # Retourne le nom du fichier sauvegardé
    
    except Exception as e:
        print(f"❌ Erreur de téléchargement d'image: {e}")
        return None


def scrape_details(url, driver, listing_id, folder_name):
    """Access a car listing page and scrape additional details including images."""
    driver.get(url)
    time.sleep(3)  # Allow the page to load
    
    # Create folder for this listing's images
    images_base_folder = os.path.join("..", "data", "avito", "images")
    os.makedirs(images_base_folder, exist_ok=True)
    
    listing_folder = os.path.join(images_base_folder, folder_name)
    os.makedirs(listing_folder, exist_ok=True)
    
    # Initialize images list
    images_paths = []
    
    try:
        # Scroll down to load all content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        # DOWNLOAD IMAGES
        try:
            # Find all images in the carousel/slider - new selector
            image_elements = driver.find_elements(By.CSS_SELECTOR, "div.picture img")
            
            # Si la sélection précédente ne trouve pas d'images, essayer d'autres sélecteurs
            if not image_elements:
                image_elements = driver.find_elements(By.CSS_SELECTOR, ".sc-1gjavk-0")
            
            if image_elements:
                print(f"✅ Found {len(image_elements)} images for listing {listing_id}")
                
                for i, img in enumerate(image_elements):
                    try:
                        img_src = img.get_attribute("src")
                        if img_src:
                            # Télécharger l'image et obtenir le chemin relatif
                            image_filename = download_image(img_src, listing_folder, f"image_{i+1}")
                            if image_filename:
                                rel_path = os.path.join(folder_name, image_filename)
                                images_paths.append(rel_path)
                                print(f"✅ Downloaded image {i+1}/{len(image_elements)} for listing {listing_id}")
                    except Exception as e:
                        print(f"⚠️ Error downloading image {i+1} for listing {listing_id}: {e}")
            else:
                print(f"⚠️ No images found for listing {listing_id}")
        except Exception as e:
            print(f"❌ Error processing images for listing {listing_id}: {e}")

        # Extraction du "Voir plus" bouton s'il existe
        try:
            show_more_button = driver.find_element(By.XPATH, "//button[contains(., 'Voir plus')]")
            driver.execute_script("arguments[0].click();", show_more_button)
            time.sleep(1)  # Attente après le clic
        except:
            pass  # Si le bouton n'est pas trouvé, continuer normalement

        # Initialize all variables to "N/A" by default
        car_type = "N/A"
        location = "N/A"
        mileage = "N/A"
        brand = "N/A"
        model = "N/A"
        doors = "N/A"
        origin = "N/A"
        first_hand = "N/A"
        fiscal_power = "N/A"
        condition = "N/A"
        equipment_text = "N/A"
        seller_city = "N/A"

        # Trouver les détails du véhicule dans la nouvelle structure
        try:
            # Obtenir la localisation du vendeur
            try:
                location_element = driver.find_element(By.XPATH, "//span[contains(@class, 'iKguVF')]")
                if location_element:
                    location = location_element.text.strip()
                    seller_city = location.split(',')[0] if ',' in location else location
            except:
                print(f"⚠️ Localisation non trouvée pour {url}")

            # Rechercher tous les éléments de caractéristiques du véhicule avec la nouvelle structure
            detail_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'sc-19cngu6-1')]")
            
            for element in detail_elements:
                try:
                    # Obtenir le texte de la valeur et le type de caractéristique
                    value_element = element.find_element(By.XPATH, ".//span[contains(@class, 'fjZBup')]")
                    label_element = element.find_element(By.XPATH, ".//span[contains(@class, 'bXFCIH')]")
                    
                    value = value_element.text.strip()
                    label = label_element.text.strip()
                    
                    # Mapping des labels aux variables
                    if "Année-Modèle" in label:
                        # Pas besoin d'extraire, déjà dans les données de base
                        pass
                    elif "Type de véhicule" in label or "Catégorie" in label:
                        car_type = value
                    elif "Kilométrage" in label:
                        mileage = value
                    elif "Marque" in label:
                        brand = value
                    elif "Modèle" in label:
                        model = value
                    elif "Nombre de portes" in label:
                        doors = value
                    elif "Origine" in label:
                        origin = value
                    elif "Première main" in label:
                        first_hand = value
                    elif "Puissance fiscale" in label:
                        fiscal_power = value
                    elif "État" in label:
                        condition = value
                    elif "Secteur" in label:
                        location = value
                
                except Exception as e:
                    continue  # Passer au prochain élément si celui-ci cause des problèmes
            
            # Extraire la catégorie si ce n'est pas encore fait
            if car_type == "N/A":
                try:
                    category_element = driver.find_element(By.XPATH, "//span[contains(@class, 'fjZBup') and preceding-sibling::span[contains(text(), 'Categorie')]]")
                    if category_element:
                        car_type = category_element.text.strip()
                except:
                    pass

            # Extraire les équipements
            try:
                equipment_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'sc-19cngu6-1')]//span[contains(@class, 'fjZBup') and not(following-sibling::span)]")
                equipment_list = []
                
                for eq in equipment_elements:
                    # Vérifier si c'est réellement un équipement et pas une autre information
                    parent = eq.find_element(By.XPATH, "./..")
                    if "Type de" not in parent.text and "Année" not in parent.text and "Marque" not in parent.text:
                        equipment_list.append(eq.text.strip())
                
                if equipment_list:
                    equipment_text = ", ".join(equipment_list)
            except Exception as e:
                print(f"⚠️ Erreur lors de l'extraction des équipements: {e}")
                
        except Exception as e:
            print(f"❌ Erreur lors de l'extraction des détails: {e}")

        return [car_type, location, mileage, brand, model, doors, origin, first_hand, fiscal_power, condition, equipment_text, seller_city, folder_name, ", ".join(images_paths)]

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return ["N/A"] * 12 + [folder_name, ""]  # Retourner le nom du dossier et chemin d'images vide en cas d'erreur


def scrape_avito():
    """Scrape the car listings on Avito for page 1 only."""
    base_url = "https://www.avito.ma/fr/maroc/voitures_d_occasion-%C3%A0_vendre"
    driver = setup_driver()
    
    data = []
    listing_id_counter = 1  # Initialize the global ID counter

    # We will only scrape page 1
    page = 1
    url = f"{base_url}?o={page}"
    print(f"🔎 Scraping page {page}: {url}")
    
    driver.get(url)
    driver.set_page_load_timeout(180)  # Increase timeout duration

    # Wait for the page to load correctly
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "sc-1nre5ec-1")))
    except Exception as e:
        print(f"❌ Timeout: Impossible de charger la page {page} ({e})")
        driver.quit()
        return []

    try:
        # Ensure all content is loaded by scrolling to the bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)  # Wait for additional content to load

        # Find the main container
        main_container = driver.find_element(By.CLASS_NAME, "sc-1nre5ec-1")

        # Get all listings on the page
        listings = main_container.find_elements(By.CSS_SELECTOR, "a.sc-1jge648-0.jZXrfL")

        if not listings:
            print(f"❌ Aucune annonce trouvée sur la page {page} !")
            driver.quit()
            return []

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


def save_to_csv(data, headers, filename):
    """Sauvegarde les données dans un fichier CSV dans ../data/avito/."""
    output_folder = os.path.join("..", "data", "avito")
    os.makedirs(output_folder, exist_ok=True)  # Create if not exists
    output_file = os.path.join(output_folder, filename)

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)

    print(f"✅ Données sauvegardées dans {output_file}")
    return output_file


def main():
    """Main function to run the complete scraper."""
    print("🚗 Starting Avito car listings scraper - Complete Process...")

    
    # Create output directory
    output_dir = os.path.join("..", "data", "avito")
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Initial scraping
    print("\n📋 Step 1: Scraping basic car listings from page 1...")
    basic_data = scrape_avito()
    
    if basic_data is None or len(basic_data) == 0:
        print("❌ No basic data found. Exiting program.")
        return
    
    print(f"✅ Found {len(basic_data)} basic listings on page 1.")
    
    # Step 2: Scrape detailed information
    print("\n📋 Step 2: Collecting detailed information and downloading images...")
    
    # Setup driver for detailed scraping
    driver = setup_driver()
    
    # Define headers for the final CSV
    complete_headers = [
        "ID", "Titre", "Prix", "Date de publication", "Année", "Type de carburant", "Transmission", "Créateur",
        "Type de véhicule", "Secteur", "Kilométrage", "Marque", "Modèle", "Nombre de portes", "Origine", 
        "Première main", "Puissance fiscale", "État", "Équipements", "Ville du vendeur", "Dossier d'images"
    ]
    
    # Prepare final data with headers
    complete_data = []
    
    # Process each listing
    for idx, row in enumerate(basic_data, start=1):
        listing_id = row[0]  # ID is at index 0
        url = row[8]         # URL is at index 8
        folder_name = row[9]  # Folder name is at index 9
        
        print(f"🔎 Processing listing {idx}/{len(basic_data)}: {url}")
        
        # Get detailed information
        details = scrape_details(url, driver, listing_id, folder_name)
        
        # Combine basic and detailed information
        combined_row = row[:8] + details
        complete_data.append(combined_row)
    
    
    driver.quit()
    

    
    # Step 3: Save complete data to CSV
    print("\n📋 Step 3: Saving complete data to CSV...")
    output_filename = "avito_complete.csv"
    save_to_csv(complete_data, complete_headers, output_filename)
    
    print("\n✅ SCRAPING PROCESS COMPLETED SUCCESSFULLY!")
    print(f"Complete data saved to: ../data/avito/{output_filename}")
    print(f"Images downloaded to: ../data/avito/images/[listing_folders]")
    print(f"Data sent to Kafka topic: {kafka_topic}")


if __name__ == "__main__":
    main()