"""
Script de scraping détaillé pour Avito Maroc (section voitures d'occasion)
--------------------------------------------------------
Ce script récupère les informations détaillées des annonces de voitures d'occasion
préalablement identifiées par le script initial_scraper.py.

Pour chaque annonce, il extrait:
- Les caractéristiques techniques complètes du véhicule (marque, modèle, kilométrage, etc.)
- Les équipements et options
- Les informations sur le vendeur et sa localisation
- Les images associées à l'annonce

Fonctionnalités:
- Traitement des annonces à partir d'un fichier CSV existant
- Téléchargement et organisation des images dans des dossiers dédiés
- Extraction de données structurées à partir des pages détaillées
- Fusion des données de base avec les informations détaillées
- Sauvegarde de l'ensemble dans un nouveau fichier CSV enrichi

Utilisation:
1. Exécutez d'abord initial_scraper.py pour collecter les URLs des annonces
2. Exécutez ensuite ce script en spécifiant la même plage de pages
3. Les résultats détaillés seront sauvegardés dans ../data/avito/
4. Les images seront stockées dans ../data/avito/images/[dossier_annonce]

Dépendances: selenium, webdriver_manager, requests, urllib, datetime, re, csv, os, time
"""




import time
import csv
import os
import re
import requests
import urllib.parse
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
            # Les images pourraient être dans différents conteneurs selon la nouvelle structure
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

        return [car_type, location, mileage, brand, model, doors, origin, first_hand, fiscal_power, condition, equipment_text, seller_city, folder_name]

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return ["N/A"] * 12 + [folder_name]  # Retourner le nom du dossier en cas d'erreur


def load_basic_data(input_file):
    """Load the basic data from the CSV file created by the initial scraper."""
    data = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                data.append(row)
        return data
    except Exception as e:
        print(f"❌ Error loading basic data: {e}")
        return None


def process_detailed_data(basic_data, output_file):
    """Process the basic data to get detailed information for each listing."""
    driver = setup_driver()

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    new_headers = [
        "ID", "Titre", "Prix", "Date de publication", "Année", "Type de carburant", "Transmission", "Créateur",
        "Type de véhicule", "Secteur", "Kilométrage", "Marque", "Modèle", "Nombre de portes", "Origine", 
        "Première main", "Puissance fiscale", "État", "Équipements", "Ville du vendeur", "Dossier d'images"
    ]

    detailed_data = [new_headers]  # Ensure new headers are used

    for idx, row in enumerate(basic_data, start=1):
        url = row[8]  # URL is at index 8
        folder_name = row[9]  # Folder name is at index 9
        print(f"🔎 Scraping listing {idx}/{len(basic_data)} : {url}")

        details = scrape_details(url, driver, idx, folder_name)

        # Merge all attributes
        combined_data = row[:8] + details  # Ensure correct ordering

        detailed_data.append(combined_data)

    driver.quit()

    # Save the extracted details
    print(f"📂 Enregistrement du fichier CSV détaillé dans : {output_file}")

    try:
        with open(output_file, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerows(detailed_data)

        print(f"✅ Fichier CSV détaillé sauvegardé dans {output_file}")

    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement des détails : {e}")


def get_page_range():
    """Demander à l'utilisateur de spécifier la plage de pages à utiliser pour le nom du fichier."""
    while True:
        try:
            print("\nVeuillez spécifier la plage de pages du fichier à utiliser:")
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
    """Main function to run the detailed scraper."""
    print("🚗 Starting Avito car listings scraper - Detail Phase...")
    
    # Demander la plage de pages pour le nom du fichier
    start_page, end_page = get_page_range()
    
    # Construire le nom du fichier d'entrée
    input_filename = f"avito_listings_p{start_page}-p{end_page}.csv"
    input_file = os.path.join("..", "data", "avito", input_filename)
    
    # Vérifier si le fichier existe
    if not os.path.exists(input_file):
        print(f"❌ Le fichier {input_file} n'existe pas!")
        print("Veuillez vérifier que vous avez bien exécuté initial_scraper.py avec les mêmes pages.")
        return
    
    # Générer le nom du fichier de sortie
    output_filename = f"avito_details_p{start_page}-p{end_page}.csv"
    output_file = os.path.join("..", "data", "avito", output_filename)
    
    print(f"\n📋 Lecture du fichier d'entrée: {input_file}")
    print(f"📝 Le fichier de sortie sera: {output_file}")
    
    # Load basic data
    basic_data = load_basic_data(input_file)
    
    if basic_data is None or len(basic_data) == 0:
        print("❌ No data found in input file. Exiting program.")
        return
    
    print(f"✅ Loaded {len(basic_data)} listings from basic data file.")
    
    # Process detailed data
    print("\n🔍 Collecting detailed information and downloading images for each listing...")
    process_detailed_data(basic_data, output_file)
    
    print("\n✅ DETAILED SCRAPING COMPLETED SUCCESSFULLY!")
    print(f"Detailed information saved to: {output_file}")
    print(f"Images downloaded to: ../data/avito/images/[listing_folders]")


if __name__ == "__main__":
    main()