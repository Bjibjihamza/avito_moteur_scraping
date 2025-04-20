"""
Scraper détaillé pour le site moteur.ma - Script d'extraction de données automobiles

Ce script permet d'extraire des informations détaillées sur les annonces de véhicules depuis le site moteur.ma.
Il utilise Selenium pour naviguer sur le site et récupérer les données complètes de chaque annonce.

Fonctionnalités principales:
- Extraction des caractéristiques détaillées des véhicules (kilométrage, marque, modèle, etc.)
- Téléchargement des images associées à chaque annonce
- Organisation des données dans un fichier CSV structuré
- Création d'une arborescence de dossiers pour stocker les images par annonce

Le script fonctionne en deux phases:
1. Lecture d'un fichier CSV contenant les données de base des annonces (généré par un scraper initial)
2. Visite de chaque page d'annonce pour extraire les informations détaillées et les images

Utilisation:
- Exécuter le script et spécifier la plage de pages concernée
- Les résultats sont enregistrés dans ../data/moteur/
"""
import os
import re
import requests
import unicodedata
import time
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver():
    """Configure et initialise le driver Selenium."""
    options = Options()
    options.add_argument("--headless")  # Exécuter sans interface graphique
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")  # Contourner la détection des bots
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def sanitize_filename(filename):
    """Nettoie un nom de fichier pour qu'il soit valide sur le système d'exploitation."""
    # Remplacer les caractères non-alphanumériques par des underscores
    filename = re.sub(r'[^\w\s-]', '_', filename)
    # Normaliser les caractères accentués
    filename = unicodedata.normalize('NFKD', filename).encode('ASCII', 'ignore').decode('ASCII')
    # Remplacer les espaces par des underscores
    filename = re.sub(r'\s+', '_', filename)
    return filename

def download_image(url, folder_path, index):
    """Télécharge une image à partir d'une URL avec des en-têtes améliorés."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        print(f"Téléchargement de {url} vers {folder_path}")
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            file_extension = url.split('.')[-1]
            if '?' in file_extension:
                file_extension = file_extension.split('?')[0]
            if not file_extension or len(file_extension) > 5:
                file_extension = "jpg"  # Extension par défaut si problème
            image_path = os.path.join(folder_path, f"image_{index}.{file_extension}")
            with open(image_path, 'wb') as f:
                f.write(response.content)
            print(f"✅ Image enregistrée : {image_path}")
            return True
        else:
            print(f"❌ Erreur HTTP {response.status_code} pour {url}")
        return False
    except Exception as e:
        print(f"⚠️ Erreur lors du téléchargement de l'image {url}: {e}")
        return False

def scrape_detail_page(driver, url, ad_id, title, price_from_csv, folder_name):
    """Scrape les détails d'une annonce spécifique avec la nouvelle structure HTML."""
    try:
        # Accéder à la page de détail
        driver.get(url)
        time.sleep(3)  # Attendre le chargement de la page
        
        # Créer un dossier pour les images de cette annonce
        images_base_folder = os.path.join("..", "data", "moteur", "images")
        os.makedirs(images_base_folder, exist_ok=True)
        
        folder_path = os.path.join(images_base_folder, folder_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"📂 Dossier créé : {folder_path}")
        
        # Initialiser avec des valeurs par défaut
        location = "N/A"
        mileage = "N/A"
        brand = "N/A"
        model = "N/A"
        doors = "N/A"
        # origin supprimé
        first_hand = "N/A"
        fiscal_power = "N/A"
        # condition supprimé
        equipment_text = "N/A"
        seller_city = "N/A"
        dedouane = "N/A"
        transmission = "N/A"  # Important: initialiser la transmission
        fuel_type = "N/A"
        creator = "N/A"  # Initialiser le créateur
        
        # Extraction du créateur
        try:
            # Rechercher le lien avec l'icône "megaphone"
            creator_element = driver.find_element(By.XPATH, "//a[contains(., 'icon-normal-megaphone')]")
            if creator_element:
                creator = creator_element.text.strip()
            
            # Si la méthode ci-dessus échoue, essayer une autre approche
            if creator == "N/A":
                creator_element = driver.find_element(By.XPATH, "//div[@class='actions block_tele']//li/a[i[contains(@class, 'icon-normal-megaphone')]]")
                creator = creator_element.text.strip()
        except Exception as e:
            print(f"Erreur extraction du créateur: {e}")
            # Troisième tentative avec un XPath plus précis
            try:
                creator_element = driver.find_element(By.XPATH, "//div[@class='block-inner block-detail-ad']//div[@class='actions block_tele']//a[contains(@href, 'stock-professionnel')]")
                creator = creator_element.text.strip()
            except Exception as e2:
                print(f"Erreur extraction du créateur (méthode alternative): {e2}")
        
        # Extraction de la transmission
        try:
            transmission_element = driver.find_element(By.XPATH, "//span[contains(text(), 'Boite de vitesses')]/following-sibling::span")
            transmission = transmission_element.text.strip()
        except Exception as e:
            print(f"Erreur extraction directe de transmission: {e}")
            transmission = "N/A"  # Garde la valeur par défaut si échec
        
        # Récupérer les détails du véhicule - PROCÉDER AVEC LA MÉTHODE HABITUELLE
        detail_lines = driver.find_elements(By.CLASS_NAME, "detail_line")
        
        for line in detail_lines:
            try:
                line_text = line.text.strip()
                spans = line.find_elements(By.TAG_NAME, "span")
                if len(spans) >= 2:
                    key = spans[0].text.strip()
                    value = spans[1].text.strip()
                    
                    if "Kilométrage" in key:
                        mileage = value
                    elif "Année" in key:
                        pass
                    elif "Boite de vitesses" in key:
                        transmission = value
                    elif "Carburant" in key:
                        fuel_type = value
                    elif "Puissance fiscale" in key:
                        fiscal_power = value
                    elif "Nombre de portes" in key:
                        doors = value
                    elif "Première main" in key:
                        first_hand = value
                    elif "Véhicule dédouané" in key:
                        dedouane = value
                    # Suppression de la condition "État"
            except Exception as e:
                print(f"Erreur lors de l'extraction d'une ligne de détail: {e}")
        
        # Extraction des équipements au lieu de la description
        try:
            equipment_elements = driver.find_elements(By.CSS_SELECTOR, "div.option_ad")
            equipment_list = []
            for element in equipment_elements:
                # Extraire le texte sans le symbole ✔
                equipment_text = element.text.strip()
                if equipment_text.startswith("✔"):
                    equipment_text = equipment_text[1:].strip()
                equipment_list.append(equipment_text)
            
            # Joindre tous les équipements en une chaîne séparée par des virgules
            equipment_text = ", ".join(equipment_list)
        except Exception as e:
            print(f"Erreur extraction équipements: {e}")
            equipment_text = "N/A"
        
        # Extraction de la ville
        try:
            city_element = driver.find_element(By.XPATH, "//a[contains(@href, 'ville')]")
            seller_city = city_element.text.strip()
            location = seller_city
        except Exception as e:
            print(f"Erreur extraction ville: {e}")
        
        # Extraction des images
        image_count = 0
        try:
            image_elements = driver.find_elements(By.CSS_SELECTOR, "img[data-u='image']")
            for index, img in enumerate(image_elements):
                img_url = img.get_attribute("src")
                if img_url and "http" in img_url:
                    success = download_image(img_url, folder_path, index + 1)
                    if success:
                        image_count += 1
        except Exception as e:
            print(f"Erreur lors de l'extraction des images: {e}")
        
        # Extraction de la marque et du modèle depuis le titre
        try:
            title_parts = title.split()
            if len(title_parts) >= 2:
                brand = title_parts[0].upper()
                model = title_parts[1].capitalize()
        except Exception as e:
            print(f"Erreur lors de l'extraction de la marque et du modèle depuis le titre: {e}")
        
        # Retourner les détails sans origine et condition
        return location, mileage, brand, model, doors, first_hand, fiscal_power, equipment_text, seller_city, folder_name, dedouane, transmission, creator
        
    except Exception as e:
        print(f"❌ Erreur lors du scraping de la page {url}: {e}")
        return ["N/A"] * 12 + ["N/A"]  # Retourner des valeurs par défaut en cas d'erreur (nombre ajusté)
def load_basic_data(input_file):
    """Charge les données de base depuis le fichier CSV."""
    data = []
    try:
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader)  # Ignorer l'en-tête
            for row in reader:
                data.append(row)
        return data
    except Exception as e:
        print(f"❌ Erreur lors du chargement des données de base: {e}")
        return None

def process_detailed_data(basic_data, output_file):
    """Traite les données de base pour obtenir des informations détaillées pour chaque annonce."""
    driver = setup_driver()

    # Nouveaux en-têtes sans "Origine" et "État"
    new_headers = [
        "ID", "Titre", "Prix", "Date de publication", "Type de carburant", "Transmission", "Créateur",
        "Secteur", "Kilométrage", "Marque", "Modèle", "Nombre de portes", 
        "Première main", "Puissance fiscale", "Équipements", "Ville du vendeur", "Dossier d'images", "Dédouané"
    ]
    
    detailed_data = [new_headers]  # Utiliser les nouveaux en-têtes

    for idx, row in enumerate(basic_data, start=1):
        try:
            ad_id = row[0]  # ID est à l'index 0
            title = row[1]  # Titre est à l'index 1
            price = row[2]  # Prix est à l'index 2
            year = row[3]   # Année est à l'index 3
            fuel_type = row[4]  # Type de carburant est à l'index 4
            city = row[5]   # Ville est à l'index 5
            url = row[6]    # URL est à l'index 6 (Lien)
            
            # Créer un nom de dossier unique pour les images
            folder_name = f"{ad_id}_{sanitize_filename(title)}"
            
            # Appeler la fonction de scraping détaillé qui retourne maintenant sans origine et condition
            car_details = scrape_detail_page(driver, url, ad_id, title, price, folder_name)
            
            # Extraire la transmission et le créateur des résultats retournés
            transmission = car_details[11] if len(car_details) > 11 else "N/A"  # Index ajusté
            creator = car_details[12] if len(car_details) > 12 else "N/A"  # Index ajusté
            
            # Créer une ligne de données combinée avec la structure modifiée (sans origine et état)
            combined_data = [
                ad_id, title, price, year, fuel_type, transmission, creator,
                car_details[0], car_details[1], car_details[2], car_details[3],
                car_details[4], car_details[5], car_details[6], car_details[7], 
                car_details[8], car_details[9], car_details[10]
            ]
            
            detailed_data.append(combined_data)
            print(f"✅ Traitement de l'annonce {idx} terminé. Créateur: {creator}")
            
        except Exception as e:
            print(f"❌ Erreur lors du traitement de l'annonce {idx}: {e}")

    driver.quit()

    # Enregistrer les détails extraits
    print(f"📂 Enregistrement du fichier CSV détaillé dans : {output_file}")
    try:
        with open(output_file, "w", newline="", encoding="utf-8-sig") as file:
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
    """Fonction principale pour exécuter le scraper détaillé."""
    print("🚗 Démarrage du scraper d'annonces moteur.ma - Phase de détail...")
    
    # Demander la plage de pages pour le nom du fichier
    start_page, end_page = get_page_range()
    
    # Construire le nom du fichier d'entrée
    input_filename = f"moteur_listings_p{start_page}-p{end_page}.csv"
    input_file = os.path.join("..", "data", "moteur", input_filename)
    
    # Vérifier si le fichier existe
    if not os.path.exists(input_file):
        print(f"❌ Le fichier {input_file} n'existe pas!")
        print("Veuillez vérifier que vous avez bien exécuté le scraper initial avec les mêmes pages.")
        return
    
    # Générer le nom du fichier de sortie
    output_filename = f"moteur_details_p{start_page}-p{end_page}.csv"
    output_file = os.path.join("..", "data", "moteur", output_filename)
    
    print(f"\n📋 Lecture du fichier d'entrée: {input_file}")
    print(f"📝 Le fichier de sortie sera: {output_file}")
    
    # Charger les données de base
    basic_data = load_basic_data(input_file)
    
    if basic_data is None or len(basic_data) == 0:
        print("❌ Aucune donnée trouvée dans le fichier d'entrée. Arrêt du programme.")
        return
    
    print(f"✅ {len(basic_data)} annonces chargées depuis le fichier de données de base.")
    
    # Traiter les données détaillées
    print("\n🔍 Collecte d'informations détaillées et téléchargement d'images pour chaque annonce...")
    process_detailed_data(basic_data, output_file)
    
    print("\n✅ SCRAPING DÉTAILLÉ TERMINÉ AVEC SUCCÈS!")
    print(f"Informations détaillées enregistrées dans: {output_file}")
    print(f"Images téléchargées dans: ../data/moteur/images/[dossiers_annonces]")

if __name__ == "__main__":
    main()