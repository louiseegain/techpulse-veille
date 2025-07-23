# airflow/dags/collecte_quotidienne.py
"""
DAG Airflow pour la collecte quotidienne de données concurrentielles TechPulse
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le dossier scrapers au path Python
sys.path.append('/opt/airflow/scrapers')

# Configuration du DAG
default_args = {
    'owner': 'techpulse-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False  # Ne pas rattraper les exécutions manquées
}

dag = DAG(
    'techpulse_collecte_quotidienne',
    default_args=default_args,
    description='Collecte quotidienne des données concurrentielles',
    schedule_interval='0 6 * * *',  # Tous les jours à 6h du matin
    max_active_runs=1,  # Une seule exécution à la fois
    tags=['techpulse', 'scraping', 'concurrence']
)

def check_database_connection():
    """Vérifier la connexion à la base de données"""
    import psycopg2
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='techpulse_veille',
            user='techpulse',
            password='techpulse2024'
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sites_concurrents WHERE actif = true;")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Connexion DB OK - {count} sites actifs")
        return f"DB_OK_{count}_sites"
        
    except Exception as e:
        logger.error(f"❌ Erreur connexion DB: {e}")
        raise Exception(f"Connexion DB échouée: {e}")

def run_cdiscount_scraping():
    """Exécuter le scraping Cdiscount avec le scraper final"""
    import logging
    import sys
    import os
    
    # Ajouter le path des scrapers
    sys.path.append('/opt/airflow/scrapers')
    
    logger = logging.getLogger(__name__)
    logger.info("🚀 Début scraping Cdiscount avec scraper final")
    
    try:
        # Import du scraper final
        from scraper_final_techpulse import TechPulseScraperFinal
        
        # Créer une instance
        scraper = TechPulseScraperFinal()
        
        # Exécuter uniquement la collecte Cdiscount
        results = scraper.run_full_collection(target_sites=['cdiscount'])
        
        if results and results.get('cdiscount', {}).get('succes', 0) > 0:
            logger.info(f"✅ Scraping Cdiscount terminé: {results['cdiscount']['succes']} produits collectés")
            return "CDISCOUNT_SUCCESS"
        else:
            raise Exception("Aucun produit collecté")
            
    except Exception as e:
        logger.error(f"❌ Erreur scraping Cdiscount: {e}")
        raise

def run_rueducommerce_scraping():
    """Exécuter le scraping Rue du Commerce avec le scraper final"""
    import logging
    import sys
    
    sys.path.append('/opt/airflow/scrapers')
    logger = logging.getLogger(__name__)
    logger.info("🚀 Début scraping Rue du Commerce avec scraper final")
    
    try:
        from scraper_final_techpulse import TechPulseScraperFinal
        scraper = TechPulseScraperFinal()
        results = scraper.run_full_collection(target_sites=['rueducommerce'])
        
        if results and results.get('rueducommerce', {}).get('succes', 0) > 0:
            logger.info(f"✅ Scraping RDC terminé: {results['rueducommerce']['succes']} produits collectés")
            return "RDC_SUCCESS"
        else:
            raise Exception("Aucun produit collecté")
            
    except Exception as e:
        logger.error(f"❌ Erreur scraping RDC: {e}")
        raise

def run_boulanger_scraping():
    """Exécuter le scraping Boulanger avec le scraper final"""
    import logging
    import sys
    
    sys.path.append('/opt/airflow/scrapers')
    logger = logging.getLogger(__name__)
    logger.info("🚀 Début scraping Boulanger avec scraper final")
    
    try:
        from scraper_final_techpulse import TechPulseScraperFinal
        scraper = TechPulseScraperFinal()
        results = scraper.run_full_collection(target_sites=['boulanger'])
        
        if results and results.get('boulanger', {}).get('succes', 0) > 0:
            logger.info(f"✅ Scraping Boulanger terminé: {results['boulanger']['succes']} produits collectés")
            return "BOULANGER_SUCCESS"
        else:
            raise Exception("Aucun produit collecté")
            
    except Exception as e:
        logger.error(f"❌ Erreur scraping Boulanger: {e}")
        raise

def generate_daily_report():
    """Générer le rapport quotidien"""
    import psycopg2
    import logging
    from datetime import date
    
    logger = logging.getLogger(__name__)
    logger.info("📊 Génération du rapport quotidien")
    
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='techpulse_veille',
            user='techpulse',
            password='techpulse2024'
        )
        
        cursor = conn.cursor()
        
        # Compter les produits collectés aujourd'hui
        cursor.execute("""
            SELECT 
                sc.nom_site,
                COUNT(*) as nb_produits
            FROM produits_concurrents pc
            JOIN sites_concurrents sc ON pc.id_site = sc.id_site
            WHERE DATE(pc.date_collecte) = %s
            GROUP BY sc.nom_site
            ORDER BY nb_produits DESC
        """, (date.today(),))
        
        results = cursor.fetchall()
        
        total_produits = sum([row[1] for row in results])
        
        rapport = f"""
📊 RAPPORT QUOTIDIEN TECHPULSE - {date.today()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 COLLECTE DU JOUR:
• Total produits collectés: {total_produits}

📋 DÉTAIL PAR SITE:
"""
        
        for site, nb in results:
            rapport += f"• {site}: {nb} produits\n"
        
        if not results:
            rapport += "• Aucune collecte aujourd'hui\n"
        
        rapport += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🤖 Rapport généré automatiquement par Airflow
"""
        
        logger.info(f"📊 Rapport généré:\n{rapport}")
        
        cursor.close()
        conn.close()
        
        return "RAPPORT_OK"
        
    except Exception as e:
        logger.error(f"❌ Erreur génération rapport: {e}")
        raise

def send_notification():
    """Envoyer une notification de fin"""
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("📧 Notification: Collecte quotidienne terminée avec succès!")
    
    # Plus tard: envoi email, Slack, etc.
    return "NOTIFICATION_SENT"

# DÉFINITION DES TÂCHES AIRFLOW

# Tâche 1: Vérification préalable
task_check_db = PythonOperator(
    task_id='check_database_connection',
    python_callable=check_database_connection,
    dag=dag
)

# Tâche 2a: Scraping Cdiscount
task_scraping_cdiscount = PythonOperator(
    task_id='scraping_cdiscount',
    python_callable=run_cdiscount_scraping,
    dag=dag
)

# Tâche 2b: Scraping Rue du Commerce  
task_scraping_rdc = PythonOperator(
    task_id='scraping_rueducommerce',
    python_callable=run_rueducommerce_scraping,
    dag=dag
)

# Tâche 2c: Scraping Boulanger
task_scraping_boulanger = PythonOperator(
    task_id='scraping_boulanger',
    python_callable=run_boulanger_scraping,
    dag=dag
)

# Tâche 3: Rapport quotidien
task_rapport = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag
)

# Tâche 4: Notification finale
task_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    dag=dag
)

# Tâche 5: Nettoyage (optionnel)
task_cleanup = BashOperator(
    task_id='cleanup_old_logs',
    bash_command="""
    echo "🧹 Nettoyage des anciens logs..."
    # Plus tard: script de nettoyage des données anciennes
    echo "✅ Nettoyage terminé"
    """,
    dag=dag
)

# DÉFINITION DES DÉPENDANCES (ordre d'exécution)

# D'abord vérifier la DB
task_check_db >> [task_scraping_cdiscount, task_scraping_rdc, task_scraping_boulanger]

# Une fois tous les scrapings terminés, générer le rapport
[task_scraping_cdiscount, task_scraping_rdc, task_scraping_boulanger] >> task_rapport

# Puis envoyer la notification et nettoyer
task_rapport >> [task_notification, task_cleanup]

# Schéma des dépendances:
#
#     check_db
#        |
#   ┌────┼────┐
#   │    │    │
# cdiscount rdc boulanger
#   │    │    │
#   └────┼────┘
#        │
#     rapport
#        |
#   ┌────┼────┐
#   │    │    │
# notification cleanup