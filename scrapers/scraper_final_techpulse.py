# scrapers/scraper_final_techpulse.py
"""
Scraper final TechPulse avec simulation de données réalistes
Pour démonstration complète du Bloc 1
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import psycopg2
from datetime import datetime, timedelta
import json
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TechPulseScraperFinal:
    """Scraper final avec simulation de données réalistes pour TechPulse"""
    
    def __init__(self):
        self.db_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'techpulse_veille',
            'user': 'techpulse',
            'password': 'techpulse2024'
        }
        
        # Catalogue produits TechPulse à surveiller
        self.produits_catalogue = [
            {
                'nom': 'iPhone 15 128GB',
                'prix_base': 829.00,
                'variation_max': 50,
                'urls': {
                    'cdiscount': 'https://www.cdiscount.com/telephonie/telephone-mobile/apple-iphone-15-128-go-noir/f-14406-app123456.html',
                    'rueducommerce': 'https://www.rueducommerce.fr/p-apple-iphone-15-128go-noir-123456.html',
                    'boulanger': 'https://www.boulanger.com/ref/apple-iphone-15-128go-noir-123456'
                }
            },
            {
                'nom': 'Samsung Galaxy S24 256GB',
                'prix_base': 759.00,
                'variation_max': 80,
                'urls': {
                    'cdiscount': 'https://www.cdiscount.com/telephonie/telephone-mobile/samsung-galaxy-s24-256go-noir/f-14406-sam789012.html',
                    'rueducommerce': 'https://www.rueducommerce.fr/p-samsung-galaxy-s24-256go-noir-789012.html',
                    'boulanger': 'https://www.boulanger.com/ref/samsung-galaxy-s24-256go-noir-789012'
                }
            },
            {
                'nom': 'MacBook Air M3 256GB',
                'prix_base': 1299.00,
                'variation_max': 100,
                'urls': {
                    'cdiscount': 'https://www.cdiscount.com/informatique/ordinateurs-pc-portables/apple-macbook-air-m3-256gb/f-10701-app345678.html',
                    'rueducommerce': 'https://www.rueducommerce.fr/p-apple-macbook-air-m3-256gb-345678.html',
                    'boulanger': 'https://www.boulanger.com/ref/apple-macbook-air-m3-256gb-345678'
                }
            },
            {
                'nom': 'iPad Air 5ème génération 64GB',
                'prix_base': 699.00,
                'variation_max': 60,
                'urls': {
                    'cdiscount': 'https://www.cdiscount.com/informatique/tablettes-tactiles/apple-ipad-air-64gb/f-10702-app567890.html',
                    'rueducommerce': 'https://www.rueducommerce.fr/p-apple-ipad-air-64gb-567890.html',
                    'boulanger': 'https://www.boulanger.com/ref/apple-ipad-air-64gb-567890'
                }
            },
            {
                'nom': 'Dell XPS 13 Plus Intel i7',
                'prix_base': 1099.00,
                'variation_max': 150,
                'urls': {
                    'cdiscount': 'https://www.cdiscount.com/informatique/ordinateurs-pc-portables/dell-xps-13-plus-i7/f-10701-del234567.html',
                    'rueducommerce': 'https://www.rueducommerce.fr/p-dell-xps-13-plus-i7-234567.html',
                    'boulanger': 'https://www.boulanger.com/ref/dell-xps-13-plus-i7-234567'
                }
            }
        ]
        
        # Mapping sites
        self.sites_mapping = {
            'cdiscount': 1,
            'rueducommerce': 2,
            'boulanger': 3
        }

    def get_db_connection(self):
        """Connexion PostgreSQL"""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Erreur connexion DB: {e}")
            return None

    def simulate_realistic_scraping(self, produit, site_name, url):
        """Simule un scraping réaliste avec variations de prix"""
        
        # Simulation du temps de scraping
        scraping_time = random.uniform(1.5, 4.0)
        logger.info(f"🔍 Scraping {site_name}: {produit['nom']}")
        time.sleep(scraping_time)
        
        try:
            # Tentative de vraie requête (pour réalisme)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=5)
            page_accessible = response.status_code == 200
            
        except:
            page_accessible = False
        
        # Génération de données réalistes
        prix_base = produit['prix_base']
        variation = random.uniform(-produit['variation_max'], produit['variation_max'])
        prix_concurrent = round(prix_base + variation, 2)
        
        # Probabilité de promotion (20% de chance)
        en_promotion = random.random() < 0.2
        prix_promotion = None
        if en_promotion:
            reduction = random.uniform(30, 100)
            prix_promotion = round(prix_concurrent - reduction, 2)
        
        # Disponibilité (95% de chance d'être disponible)
        disponible = random.random() < 0.95
        
        # Note et avis (simulation réaliste)
        note_moyenne = round(random.uniform(3.5, 4.8), 1)
        nombre_avis = random.randint(50, 2000)
        
        # Données de stock
        if disponible:
            stock_options = ["En stock", "Livraison 24h", "Disponible", f"Stock: {random.randint(5, 50)} unités"]
            stock_affiche = random.choice(stock_options)
        else:
            stock_affiche = "Rupture de stock temporaire"
        
        product_data = {
            'url': url,
            'nom_produit': f"{produit['nom']} - {site_name.title()}",
            'prix_ttc': prix_concurrent,
            'prix_promotion': prix_promotion,
            'en_promotion': en_promotion,
            'disponible': disponible,
            'note_moyenne': note_moyenne,
            'nombre_avis': nombre_avis,
            'stock_affiche': stock_affiche,
            'date_collecte': datetime.now(),
            'page_accessible': page_accessible
        }
        
        # Checksum
        data_string = f"{product_data['nom_produit']}_{product_data['prix_ttc']}_{product_data['disponible']}"
        product_data['checksum_produit'] = hashlib.md5(data_string.encode()).hexdigest()
        
        # Log détaillé
        status_icon = "🟢" if disponible else "🔴"
        promo_info = f" (PROMO: {prix_promotion}€)" if en_promotion else ""
        logger.info(f"  {status_icon} {prix_concurrent}€{promo_info} - {note_moyenne}⭐ ({nombre_avis} avis)")
        
        return product_data

    def save_to_database(self, product_data, site_name):
        """Sauvegarde en base de données"""
        conn = self.get_db_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            site_id = self.sites_mapping.get(site_name, 1)
            
            # Insertion dans produits_concurrents
            insert_query = """
                INSERT INTO produits_concurrents (
                    id_site, url_produit, nom_produit_concurrent,
                    prix_ttc, prix_promotion, en_promotion, disponible, stock_affiche,
                    note_moyenne, nombre_avis, date_collecte, donnees_brutes, checksum_produit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            donnees_brutes = {
                'page_accessible': product_data.get('page_accessible', False),
                'simulation': True,
                'scraping_timestamp': product_data['date_collecte'].isoformat()
            }
            
            cursor.execute(insert_query, (
                site_id,
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
                json.dumps(donnees_brutes),
                product_data['checksum_produit']
            ))
            
            # Mise à jour historique des prix
            cursor.execute("""
                INSERT INTO historique_prix (
                    id_site, prix_ttc, prix_promotion, en_promotion, date_prix
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                site_id,
                product_data['prix_ttc'],
                product_data['prix_promotion'],
                product_data['en_promotion'],
                product_data['date_collecte'].date()
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur sauvegarde: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def log_scraping_session(self, site_name, nb_produits, nb_succes, nb_erreurs, duree):
        """Log de la session de scraping"""
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            
            site_id = self.sites_mapping.get(site_name, 1)
            statut = 'success' if nb_erreurs == 0 else 'warning' if nb_erreurs < nb_produits else 'error'
            
            cursor.execute("""
                INSERT INTO logs_collecte (
                    id_site, type_collecte, statut, nb_produits_collectes,
                    nb_erreurs, duree_execution_sec, date_debut, date_fin
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                site_id,
                'scraping_simulation',
                statut,
                nb_succes,
                nb_erreurs,
                int(duree),
                datetime.now() - timedelta(seconds=duree),
                datetime.now()
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Erreur log session: {e}")
        finally:
            conn.close()

    def run_full_collection(self, target_sites=None):
        """Exécute une collecte complète sur tous les sites"""
        if target_sites is None:
            target_sites = ['cdiscount', 'rueducommerce', 'boulanger']
        
        logger.info(f"🚀 Début collecte complète TechPulse")
        logger.info(f"📋 Sites ciblés: {', '.join(target_sites)}")
        logger.info(f"📦 Produits à collecter: {len(self.produits_catalogue)}")
        
        total_start_time = time.time()
        results_summary = {}
        
        for site_name in target_sites:
            logger.info(f"\n📡 === COLLECTE {site_name.upper()} ===")
            
            site_start_time = time.time()
            nb_succes = 0
            nb_erreurs = 0
            
            for produit in self.produits_catalogue:
                if site_name in produit['urls']:
                    url = produit['urls'][site_name]
                    
                    try:
                        # Scraping
                        product_data = self.simulate_realistic_scraping(produit, site_name, url)
                        
                        # Sauvegarde
                        if self.save_to_database(product_data, site_name):
                            nb_succes += 1
                        else:
                            nb_erreurs += 1
                        
                        # Délai entre produits
                        time.sleep(random.uniform(1, 3))
                        
                    except Exception as e:
                        logger.error(f"❌ Erreur {produit['nom']}: {e}")
                        nb_erreurs += 1
            
            # Log de la session
            site_duration = time.time() - site_start_time
            self.log_scraping_session(site_name, len(self.produits_catalogue), nb_succes, nb_erreurs, site_duration)
            
            # Résumé du site
            results_summary[site_name] = {
                'succes': nb_succes,
                'erreurs': nb_erreurs,
                'duree': site_duration
            }
            
            logger.info(f"✅ {site_name}: {nb_succes} succès, {nb_erreurs} erreurs en {site_duration:.1f}s")
        
        # Résumé global
        total_duration = time.time() - total_start_time
        total_succes = sum(r['succes'] for r in results_summary.values())
        total_erreurs = sum(r['erreurs'] for r in results_summary.values())
        
        logger.info(f"\n📊 === RÉSUMÉ GLOBAL ===")
        logger.info(f"🎯 Total collecté: {total_succes} produits")
        logger.info(f"❌ Erreurs: {total_erreurs}")
        logger.info(f"⏱️ Durée totale: {total_duration:.1f}s")
        logger.info(f"📈 Taux de succès: {(total_succes/(total_succes+total_erreurs)*100):.1f}%")
        
        return results_summary

def test_collecte_complete():
    """Test de collecte complète"""
    scraper = TechPulseScraperFinal()
    
    # Collecte sur les 3 sites
    results = scraper.run_full_collection()
    
    logger.info("\n🎉 Collecte terminée avec succès !")
    logger.info("📋 Vérifiez les données dans pgAdmin:")
    logger.info("   - Table 'produits_concurrents' pour les produits")
    logger.info("   - Table 'logs_collecte' pour les sessions")
    logger.info("   - Table 'historique_prix' pour l'évolution")

if __name__ == "__main__":
    test_collecte_complete()