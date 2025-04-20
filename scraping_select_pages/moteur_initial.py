"""
Script de scraping pour Moteur.ma (section voitures d'occasion)
--------------------------------------------------------
Ce script permet d'extraire les annonces de voitures d'occasion du site Moteur.ma.
Il collecte les informations de base pour chaque annonce sur plusieurs pages:
- Identifiant de l'annonce
- Titre de l'annonce
- Prix
- Année du véhicule
- Type de carburant
- Ville du vendeur
- URL de l'annonce

Fonctionnalités:
- Navigation automatique entre les pages spécifiées par l'utilisateur
- Extraction des données structurées à partir des listings
- Identification automatique des paramètres du véhicule
- Détection des identifiants d'annonces à partir des URLs
- Filtrage des valeurs non pertinentes
- Sauvegarde des données collectées au format CSV avec pandas

Utilisation:
1. Exécutez le script
2. Entrez la plage de pages à scraper lorsque demandé
3. Les résultats seront sauvegardés dans ../data/moteur/

Dépendances: selenium, webdriver_manager, pandas, os, re, time, datetime
"""

import os
import re
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# Configuration des répertoires
DATA_DIR = "../data/moteur"
os.makedirs(DATA_DIR, exist_ok=True)

def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--log-level=3")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

BASE_URL = "https://www.moteur.ma/fr/voiture/achat-voiture-occasion/"

def extract_id_from_url(url):
    match = re.search(r"/detail-annonce/(\d+)/", url)
    return match.group(1) if match else "N/A"

def scrape_page(driver, page_number):
    offset = (page_number - 1) * 30
    page_url = f"{BASE_URL}{offset}" if offset > 0 else BASE_URL

    print(f"🔎 Scraping page {page_number}: {page_url}")
    driver.get(page_url)

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "row-item"))
        )
    except:
        print(f"❌ Aucune annonce trouvée sur la page {page_number} !")
        return []

    car_elements = driver.find_elements(By.CLASS_NAME, "row-item")
    print(f"✅ {len(car_elements)} annonces trouvées sur la page {page_number} !")

    data = []

    for car in car_elements:
        try:
            title_element = car.find_element(By.CLASS_NAME, "title_mark_model")
            title = title_element.text.strip() if title_element else "N/A"

            try:
                link_element = car.find_element(By.XPATH, ".//h3[@class='title_mark_model']/a")
                link = link_element.get_attribute("href") if link_element else "N/A"
                ad_id = extract_id_from_url(link)
            except:
                link, ad_id = "N/A", "N/A"

            try:
                price_element = car.find_element(By.CLASS_NAME, "PriceListing")
                price = price_element.text.strip()
            except:
                price = "N/A"

            meta_elements = car.find_elements(By.TAG_NAME, "li")
            year = "N/A"
            city = "N/A"
            fuel = "N/A"

            # Liste des valeurs à ignorer comme fausses villes
            forbidden_values = ["Appeler pour le prix", "Se faire rappeler", "Booster l'annonce", ""]

            for li in meta_elements:
                text = li.text.strip()

                if re.match(r"^(19|20)\d{2}$", text):
                    year = text
                elif text.lower() in ["essence", "diesel", "hybride", "électrique"]:
                    fuel = text.capitalize()
                elif city == "N/A" and text not in forbidden_values:
                    city = text

            if city in forbidden_values or city == "N/A":
                print(f"⚠️ Ville douteuse pour l'annonce ID {ad_id} : '{city}' - {link}")

            data.append({
                "ID": ad_id,
                "Titre": title,
                "Prix": price,
                "Année": year,
                "Type de carburant": fuel,
                "Ville": city,
                "URL de l'annonce": link
            })

        except Exception as e:
            print(f"⚠️ Erreur avec une annonce: {e}")

    time.sleep(3)
    return data

def get_page_range():
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

def save_to_csv(data, filename):
    output_file = os.path.join(DATA_DIR, filename)
    columns = ["ID", "Titre", "Prix", "Année", "Type de carburant", "Ville", "URL de l'annonce"]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"✅ Données sauvegardées dans {output_file}")
    return output_file

def main():
    print("🚗 Starting Moteur.ma car listings scraper...")
    start_page, end_page = get_page_range()
    print(f"\n📋 Scraping car listings from page {start_page} to page {end_page}...")

    driver = setup_driver()
    try:
        all_data = []
        for page_number in range(start_page, end_page + 1):
            page_data = scrape_page(driver, page_number)
            all_data.extend(page_data)

        if not all_data:
            print("❌ No data found. Exiting program.")
            return

        file_suffix = f"p{start_page}-p{end_page}"
        output_csv = save_to_csv(all_data, f"moteur_listings_{file_suffix}.csv")

        print("\n✅ SCRAPING COMPLETED SUCCESSFULLY!")
        print(f"Found {len(all_data)} listings across {end_page - start_page + 1} pages.")
        print(f"Listings saved to: {output_csv}")

    except Exception as e:
        print(f"❌ Erreur globale: {e}")
    finally:
        driver.quit()
        print("🏁 Programme terminé.")

if __name__ == "__main__":
    main()
