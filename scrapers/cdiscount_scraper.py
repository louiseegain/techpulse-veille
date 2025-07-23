# scrapers/cdiscount_scraper_v2.py
"""
Scraper Cdiscount amélioré avec sélecteurs CSS réels
Version simplifiée et robuste pour TechPulse
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import psycopg2
from datetime import datetime
import json
import hashlib
from fake_useragent import UserAgent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CdiscountScraperV2:
    """Scraper Cdiscount amélioré"""
    
    def __init__(self):
        self.session = requests.Session()
        self.ua = UserAgent()
        self.base_url = "https://www.cdiscount.com"
        self.site_id = 1
        
        # Headers plus réalistes
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        })
        
        # Config DB
        self.db_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'techpulse_veille',
            'user': 'techpulse',
            'password': 'techpulse2024'
        }
    
    def get_db_connection(self):
        """Connexion PostgreSQL"""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Erreur connexion DB: {e}")
            return None
    
    def delay_between_requests(self, min_delay=3, max_delay=7):
        """Délai aléatoire pour éviter détection"""
        delay = random.uniform(min_delay, max_delay)
        logger.info(f"⏱️  Attente {delay:.1f}s...")
        time.sleep(delay)
    
    def scrape_product_page(self, product_url):
        """Scrape une page produit avec gestion d'erreurs robuste"""
        try:
            logger.info(f"🔍 Scraping: {product_url}")
            
            # Requête avec retry
            for attempt in range(3):
                try:
                    response = self.session.get(product_url, timeout=15)
                    if response.status_code == 200:
                        break
                    elif response.status_code == 403:
                        logger.warning(f"⚠️  Accès refusé (403) - tentative {attempt + 1}/3")
                        time.sleep(5)
                    else:
                        logger.warning(f"⚠️  Status {response.status_code} - tentative {attempt + 1}/3")
                        time.sleep(2)
                except requests.RequestException as e:
                    logger.warning(f"⚠️  Erreur requête {attempt + 1}/3: {e}")
                    time.sleep(5)
            else:
                logger.error(f"❌ Échec après 3 tentatives pour {product_url}")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraction des données avec fallbacks multiples
            product_data = {
                'url': product_url,
                'nom_produit': self._extract_product_name(soup),
                'prix_ttc': self._extract_price(soup),
                'prix_promotion': None,  # Simplifié pour l'instant
                'en_promotion': False,   # Simplifié pour l'instant
                'disponible': self._extract_availability(soup),
                'note_moyenne': None,    # Simplifié pour l'instant
                'nombre_avis': 0,        # Simplifié pour l'instant
                'stock_affiche': "Non spécifié",
                'date_collecte': datetime.now(),
                'donnees_brutes': str(soup)[:500]  # Échantillon pour debug
            }
            
            # Checksum
            data_string = f"{product_data['nom_produit']}_{product_data['prix_ttc']}_{product_data['disponible']}"
            product_data['checksum_produit'] = hashlib.md5(data_string.encode()).hexdigest()
            
            logger.info(f"✅ Produit scrapé: {product_data['nom_produit'][:50]} - {product_data['prix_ttc']}€")
            return product_data
            
        except Exception as e:
            logger.error(f"❌ Erreur scraping {product_url}: {e}")
            return None
    
    def _extract_product_name(self, soup):
        """Extraction nom produit avec fallbacks multiples"""
        # Sélecteurs CSS possibles pour Cdiscount
        selectors = [
            'h1[data-product="name"]',
            'h1.fpHdrDsc',
            'h1.product-title',
            '.fpHdr h1',
            'h1.title',
            'h1',
            '.product-name h1',
            '[data-testid="product-title"]'
        ]
        
        for selector in selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    name = elements[0].get_text(strip=True)
                    if name and len(name) > 3:  # Nom valide
                        return name[:255]  # Limite DB
            except:
                continue
        
        # Fallback: chercher dans le title de la page
        try:
            title = soup.find('title')
            if title:
                title_text = title.get_text(strip=True)
                if 'cdiscount' in title_text.lower():
                    # Nettoyer le titre
                    clean_title = title_text.replace(' - Cdiscount', '').replace(' | Cdiscount', '')
                    return clean_title[:255]
        except:
            pass
        
        return "Nom non trouvé"
    
    def _extract_price(self, soup):
        """Extraction prix avec multiples stratégies"""
        # Stratégie 1: Sélecteurs CSS
        price_selectors = [
            '.fpPrice .price',
            '.price-current',
            '.product-price .price',
            '[data-price]',
            '.fpPrice',
            '.price',
            '.current-price',
            '.sale-price'
        ]
        
        for selector in price_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    price_text = element.get_text(strip=True)
                    price = self._parse_price(price_text)
                    if price and price > 0:
                        return price
                    
                    # Vérifier attribut data-price
                    data_price = element.get('data-price')
                    if data_price:
                        price = self._parse_price(data_price)
                        if price and price > 0:
                            return price
            except:
                continue
        
        # Stratégie 2: Recherche dans le texte général
        try:
            page_text = soup.get_text()
            import re
            # Chercher des patterns de prix français
            price_patterns = [
                r'(\d{1,4})[,.](\d{2})\s*€',
                r'(\d{1,4})\s*€',
                r'EUR\s*(\d{1,4})[,.](\d{2})',
            ]
            
            for pattern in price_patterns:
                matches = re.findall(pattern, page_text)
                for match in matches:
                    if isinstance(match, tuple):
                        if len(match) == 2:
                            price = float(f"{match[0]}.{match[1]}")
                        else:
                            price = float(match[0])
                    else:
                        price = float(match)
                    
                    # Vérifier que c'est un prix réaliste (entre 1€ et 5000€)
                    if 1 <= price <= 5000:
                        return price
        except:
            pass
        
        return None
    
    def _extract_availability(self, soup):
        """Extraction disponibilité"""
        # Indicateurs de disponibilité
        availability_indicators = [
            'en stock',
            'disponible',
            'available',
            'livraison',
            'expedie'
        ]
        
        unavailability_indicators = [
            'rupture',
            'indisponible',
            'non disponible',
            'out of stock',
            'épuisé'
        ]
        
        try:
            page_text = soup.get_text().lower()
            
            # Chercher indicateurs de rupture (priorité)
            for indicator in unavailability_indicators:
                if indicator in page_text:
                    return False
            
            # Chercher indicateurs de disponibilité
            for indicator in availability_indicators:
                if indicator in page_text:
                    return True
                    
        except:
            pass
        
        # Par défaut, considérer comme disponible
        return True
    
    def _parse_price(self, price_text):
        """Parse texte prix en float"""
        if not price_text:
            return None
        
        try:
            import re
            # Nettoyer: garder chiffres, virgules, points
            clean_price = re.sub(r'[^\d,.]', '', str(price_text))
            
            if not clean_price:
                return None
            
            # Gérer formats français
            if ',' in clean_price and '.' in clean_price:
                # Format 1.234,56
                clean_price = clean_price.replace('.', '').replace(',', '.')
            elif ',' in clean_price:
                # Format 123,45
                clean_price = clean_price.replace(',', '.')
            
            price = float(clean_price)
            
            # Vérifier prix réaliste
            if 1 <= price <= 10000:
                return price
            else:
                return None
                
        except (ValueError, TypeError):
            return None
    
    def save_to_database(self, product_data, id_produit_techpulse=None):
        """Sauvegarde en base"""
        if not product_data:
            return False
        
        conn = self.get_db_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Requête simplifiée (sans ON CONFLICT complexe)
            insert_query = """
                INSERT INTO produits_concurrents (
                    id_produit_techpulse, id_site, url_produit, nom_produit_concurrent,
                    prix_ttc, prix_promotion, en_promotion, disponible, stock_affiche,
                    note_moyenne, nombre_avis, date_collecte, donnees_brutes, checksum_produit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                id_produit_techpulse,
                self.site_id,
                product_data['url'],
                product_data['nom_produit'],
                product_data['prix_ttc'],
                product_data['prix_promotion'],
                product_data['en_promotion'],
                product_data['disponible'],
                product_data['stock_affiche'],
                product_data['note_moyenne'],
                product_data['nombre_avis'],
                product_data['date_collecte'],
                json.dumps({'sample': product_data['donnees_brutes'][:200]}),
                product_data['checksum_produit']
            ))
            
            conn.commit()
            logger.info("✅ Données sauvegardées en base")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur sauvegarde DB: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

def test_scraper_v2():
    """Test du scraper amélioré"""
    logger.info("🚀 Test scraper Cdiscount V2")
    
    scraper = CdiscountScraperV2()
    
    # URLs de test simplifiées (pages qui existent vraiment)
    test_urls = [
        "https://www.cdiscount.com/telephonie/telephone-mobile/apple-iphone-15-128-go-noir/f-14406-app0195949102080.html",
    ]
    
    nb_succes = 0
    nb_erreurs = 0
    
    for url in test_urls:
        try:
            product_data = scraper.scrape_product_page(url)
            
            if product_data and product_data['prix_ttc']:
                success = scraper.save_to_database(product_data)
                if success:
                    nb_succes += 1
                else:
                    nb_erreurs += 1
            else:
                nb_erreurs += 1
            
            scraper.delay_between_requests(2, 4)
            
        except Exception as e:
            logger.error(f"❌ Erreur test {url}: {e}")
            nb_erreurs += 1
    
    logger.info(f"📊 Test V2 terminé: {nb_succes} succès, {nb_erreurs} erreurs")

if __name__ == "__main__":
    test_scraper_v2()