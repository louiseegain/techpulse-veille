# scrapers/test_reel_cdiscount.py
"""
Test avec inspection réelle d'une page Cdiscount
Pour analyser la structure HTML et ajuster nos sélecteurs
"""

import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_cdiscount_page():
    """Analyser la structure d'une page Cdiscount pour ajuster les sélecteurs"""
    
    # Page d'accueil Cdiscount ou page de recherche
    test_urls = [
        "https://www.cdiscount.com/",
        "https://www.cdiscount.com/search/10/iphone.html",
        "https://www.cdiscount.com/telephonie/telephone-mobile/mp-14406.html"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    for url in test_urls:
        try:
            logger.info(f"🔍 Test connexion: {url}")
            
            response = session.get(url, timeout=10)
            logger.info(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Analyser la structure
                title = soup.find('title')
                logger.info(f"Title: {title.get_text()[:100] if title else 'Non trouvé'}")
                
                # Chercher des éléments de prix
                price_elements = []
                
                # Sélecteurs possibles
                price_selectors = [
                    '.price', '.fpPrice', '[class*="price"]', 
                    '[data-price]', '.current-price', '.sale-price'
                ]
                
                for selector in price_selectors:
                    elements = soup.select(selector)
                    if elements:
                        logger.info(f"Trouvé {len(elements)} éléments avec '{selector}'")
                        for i, elem in enumerate(elements[:3]):  # Max 3 exemples
                            text = elem.get_text(strip=True)[:50]
                            if text:
                                logger.info(f"  Exemple {i+1}: {text}")
                
                # Chercher des liens produits
                product_links = soup.select('a[href*="/f-"]')  # Format typique Cdiscount
                if product_links:
                    logger.info(f"Trouvé {len(product_links)} liens produits")
                    for i, link in enumerate(product_links[:3]):
                        href = link.get('href', '')
                        if href.startswith('/'):
                            href = 'https://www.cdiscount.com' + href
                        logger.info(f"  Produit {i+1}: {href}")
                
                return True
                
            else:
                logger.warning(f"Status non-200: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Erreur {url}: {e}")
    
    return False

def test_simple_product_extraction():
    """Test d'extraction sur une page produit simple"""
    
    # Utilisons une page de recherche d'abord pour trouver des vrais liens
    search_url = "https://www.cdiscount.com/search/10/smartphone.html"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        logger.info(f"🔍 Recherche produits: {search_url}")
        
        response = requests.get(search_url, headers=headers, timeout=10)
        logger.info(f"Status recherche: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraire des informations basiques
            title = soup.find('title')
            if title:
                logger.info(f"✅ Page title: {title.get_text()}")
            
            # Chercher du contenu avec "smartphone" ou "iPhone"
            page_text = soup.get_text().lower()
            if 'smartphone' in page_text or 'iphone' in page_text:
                logger.info("✅ Contenu pertinent trouvé sur la page")
            else:
                logger.warning("⚠️ Pas de contenu smartphone trouvé")
            
            # Chercher des prix (patterns simples)
            import re
            price_matches = re.findall(r'(\d{1,4})[,.](\d{2})\s*€', page_text)
            if price_matches:
                logger.info(f"✅ Prix trouvés: {price_matches[:5]}")
            else:
                logger.warning("⚠️ Aucun prix trouvé")
            
            return True
            
    except Exception as e:
        logger.error(f"Erreur test extraction: {e}")
    
    return False

def test_db_insertion():
    """Test d'insertion d'un produit fictif mais réaliste"""
    import psycopg2
    from datetime import datetime
    import json
    
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='techpulse_veille',
            user='techpulse',
            password='techpulse2024'
        )
        
        cursor = conn.cursor()
        
        # Insérer un produit "scrapé" de test
        cursor.execute("""
            INSERT INTO produits_concurrents (
                id_site, url_produit, nom_produit_concurrent,
                prix_ttc, disponible, date_collecte, checksum_produit
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            1,  # Cdiscount
            'https://www.cdiscount.com/test-scraped-product',
            'Smartphone Test Scrapé depuis page réelle',
            299.99,
            True,
            datetime.now(),
            'test_checksum_' + str(datetime.now().timestamp())
        ))
        
        conn.commit()
        
        # Vérifier l'insertion
        cursor.execute("""
            SELECT nom_produit_concurrent, prix_ttc, date_collecte 
            FROM produits_concurrents 
            WHERE url_produit = %s
        """, ('https://www.cdiscount.com/test-scraped-product',))
        
        result = cursor.fetchone()
        if result:
            logger.info(f"✅ Produit inséré: {result[0]} - {result[1]}€")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur DB: {e}")
        return False

def main():
    """Test principal pour valider l'approche"""
    logger.info("🚀 Analyse et test réel Cdiscount")
    
    # Test 1: Analyser structure Cdiscount
    logger.info("\n--- Test 1: Analyse structure Cdiscount ---")
    success1 = analyze_cdiscount_page()
    
    # Test 2: Extraction simple
    logger.info("\n--- Test 2: Extraction simple ---")
    success2 = test_simple_product_extraction()
    
    # Test 3: Insertion DB
    logger.info("\n--- Test 3: Test insertion DB ---")
    success3 = test_db_insertion()
    
    # Résumé
    logger.info(f"\n📊 Résultats tests:")
    logger.info(f"  Structure Cdiscount: {'✅' if success1 else '❌'}")
    logger.info(f"  Extraction simple: {'✅' if success2 else '❌'}")
    logger.info(f"  Insertion DB: {'✅' if success3 else '❌'}")
    
    if success3:
        logger.info("\n🎯 Recommandation: Le système fonctionne !")
        logger.info("   Prochaine étape: Améliorer les sélecteurs CSS pour Cdiscount")
    else:
        logger.error("\n❌ Problèmes à résoudre avant de continuer")

if __name__ == "__main__":
    main()