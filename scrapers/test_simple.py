# scrapers/test_simple.py
"""
Test scraper simplifié pour valider la connexion DB et le scraping de base
"""

import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_db_connection():
    """Test de connexion à la base de données"""
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='techpulse_veille',
            user='techpulse',
            password='techpulse2024'
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sites_concurrents;")
        count = cursor.fetchone()[0]
        logger.info(f"✅ Connexion DB OK - {count} sites concurrents en base")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur connexion DB: {e}")
        return False

def test_simple_scraping():
    """Test de scraping basique"""
    try:
        # Test sur une page simple
        url = "https://httpbin.org/html"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, timeout=10, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Chercher n'importe quel élément
        title = soup.find('title') or soup.find('h1') or soup.find('body')
        
        if title:
            logger.info(f"✅ Scraping OK - Élément trouvé: {title.name} - {title.get_text()[:50]}...")
            return True
        else:
            logger.error("❌ Aucun élément trouvé")
            logger.info(f"HTML reçu: {response.text[:200]}...")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erreur scraping: {e}")
        return False

def insert_test_data():
    """Insertion de données de test"""
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='techpulse_veille',
            user='techpulse',
            password='techpulse2024'
        )
        
        cursor = conn.cursor()
        
        # Insertion d'un produit concurrent de test
        cursor.execute("""
            INSERT INTO produits_concurrents (
                id_site, url_produit, nom_produit_concurrent, prix_ttc, 
                disponible, date_collecte
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            1,  # Cdiscount
            'https://test.com/produit1',
            'Produit Test iPhone',
            799.99,
            True,
            datetime.now()
        ))
        
        # Log de collecte
        cursor.execute("""
            INSERT INTO logs_collecte (
                id_site, type_collecte, statut, nb_produits_collectes
            ) VALUES (%s, %s, %s, %s)
        """, (
            1,
            'test',
            'success',
            1
        ))
        
        conn.commit()
        logger.info("✅ Données de test insérées avec succès")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur insertion: {e}")
        return False

def main():
    """Test principal"""
    logger.info("🚀 Test scraper simplifié")
    
    # Test 1: Connexion DB
    if not test_db_connection():
        return
    
    # Test 2: Scraping basique
    if not test_simple_scraping():
        return
    
    # Test 3: Insertion de données
    if not insert_test_data():
        return
    
    logger.info("🎉 Tous les tests passés avec succès !")

if __name__ == "__main__":
    main()