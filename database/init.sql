-- Initialisation base de données TechPulse

-- Création de la base airflow pour Airflow
CREATE DATABASE airflow_db;

-- Connexion à la base principale
\c techpulse_veille;

-- Extension pour UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table des sites concurrents
CREATE TABLE sites_concurrents (
    id_site SERIAL PRIMARY KEY,
    nom_site VARCHAR(100) NOT NULL UNIQUE,
    url_base VARCHAR(255) NOT NULL,
    actif BOOLEAN DEFAULT true,
    delai_requete_sec INTEGER DEFAULT 3,
    user_agent VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des catégories de produits
CREATE TABLE categories (
    id_categorie SERIAL PRIMARY KEY,
    nom_categorie VARCHAR(100) NOT NULL,
    description TEXT,
    actif BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des marques
CREATE TABLE marques (
    id_marque SERIAL PRIMARY KEY,
    nom_marque VARCHAR(100) NOT NULL,
    actif BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des produits TechPulse (référentiel interne)
CREATE TABLE produits_techpulse (
    id_produit SERIAL PRIMARY KEY,
    reference_interne VARCHAR(50) NOT NULL UNIQUE,
    nom_produit VARCHAR(255) NOT NULL,
    id_categorie INTEGER REFERENCES categories(id_categorie),
    id_marque INTEGER REFERENCES marques(id_marque),
    prix_vente_ttc DECIMAL(10,2),
    stock_disponible INTEGER DEFAULT 0,
    ean VARCHAR(13),
    actif BOOLEAN DEFAULT true,
    priorite_veille VARCHAR(20) DEFAULT 'normale', -- haute, normale, faible
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des produits concurrents collectés
CREATE TABLE produits_concurrents (
    id_produit_concurrent SERIAL PRIMARY KEY,
    id_produit_techpulse INTEGER REFERENCES produits_techpulse(id_produit),
    id_site INTEGER REFERENCES sites_concurrents(id_site),
    url_produit VARCHAR(500) NOT NULL,
    nom_produit_concurrent VARCHAR(255),
    reference_concurrent VARCHAR(100),
    prix_ttc DECIMAL(10,2),
    prix_promotion DECIMAL(10,2),
    en_promotion BOOLEAN DEFAULT false,
    disponible BOOLEAN DEFAULT true,
    stock_affiche VARCHAR(50),
    note_moyenne DECIMAL(3,2),
    nombre_avis INTEGER DEFAULT 0,
    date_collecte TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    donnees_brutes JSONB, -- Stockage des données complètes en JSON
    checksum_produit VARCHAR(64), -- Hash pour détecter les changements
    INDEX(date_collecte),
    INDEX(id_produit_techpulse, date_collecte),
    UNIQUE(id_site, url_produit, date_collecte::date)
);

-- Table de l'historique des prix (optimisée pour les analytics)
CREATE TABLE historique_prix (
    id_historique SERIAL PRIMARY KEY,
    id_produit_techpulse INTEGER REFERENCES produits_techpulse(id_produit),
    id_site INTEGER REFERENCES sites_concurrents(id_site),
    prix_ttc DECIMAL(10,2) NOT NULL,
    prix_promotion DECIMAL(10,2),
    en_promotion BOOLEAN DEFAULT false,
    date_prix DATE NOT NULL,
    heure_collecte TIME DEFAULT CURRENT_TIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(id_produit_techpulse, id_site, date_prix),
    INDEX(date_prix),
    INDEX(id_produit_techpulse, date_prix)
);

-- Table des logs de collecte
CREATE TABLE logs_collecte (
    id_log SERIAL PRIMARY KEY,
    id_site INTEGER REFERENCES sites_concurrents(id_site),
    type_collecte VARCHAR(50), -- scraping, api, manuel
    statut VARCHAR(20), -- success, error, warning
    nb_produits_collectes INTEGER DEFAULT 0,
    nb_erreurs INTEGER DEFAULT 0,
    duree_execution_sec INTEGER,
    message_erreur TEXT,
    details_execution JSONB,
    date_debut TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_fin TIMESTAMP,
    INDEX(date_debut),
    INDEX(id_site, date_debut)
);

-- Table des alertes prix
CREATE TABLE alertes_prix (
    id_alerte SERIAL PRIMARY KEY,
    id_produit_techpulse INTEGER REFERENCES produits_techpulse(id_produit),
    id_site INTEGER REFERENCES sites_concurrents(id_site),
    type_alerte VARCHAR(50), -- baisse_prix, rupture_stock, nouveau_concurrent
    ancien_prix DECIMAL(10,2),
    nouveau_prix DECIMAL(10,2),
    pourcentage_variation DECIMAL(5,2),
    statut VARCHAR(20) DEFAULT 'nouveau', -- nouveau, traite, ignore
    date_alerte TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_traitement TIMESTAMP,
    commentaire TEXT,
    INDEX(date_alerte),
    INDEX(statut, date_alerte)
);

-- Données initiales
INSERT INTO sites_concurrents (nom_site, url_base, delai_requete_sec, user_agent) VALUES
('Cdiscount', 'https://www.cdiscount.com', 3, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
('Rue du Commerce', 'https://www.rueducommerce.fr', 4, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
('Boulanger', 'https://www.boulanger.com', 3, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

INSERT INTO categories (nom_categorie, description) VALUES
('Smartphones', 'Téléphones portables et smartphones'),
('Ordinateurs portables', 'PC portables toutes marques'),
('Tablettes', 'Tablettes tactiles et e-readers'),
('Accessoires informatique', 'Claviers, souris, écrans, etc.'),
('Audio', 'Casques, enceintes, audio portable');

INSERT INTO marques (nom_marque) VALUES
('Apple'), ('Samsung'), ('Huawei'), ('Xiaomi'), ('OnePlus'),
('Dell'), ('HP'), ('Lenovo'), ('Asus'), ('Acer'),
('Sony'), ('JBL'), ('Bose'), ('Sennheiser');

-- Exemples de produits TechPulse (à adapter selon votre catalogue)
INSERT INTO produits_techpulse (reference_interne, nom_produit, id_categorie, id_marque, prix_vente_ttc, priorite_veille) VALUES
('TP-IP14-128', 'iPhone 14 128GB', 1, 1, 829.00, 'haute'),
('TP-SM-S23', 'Samsung Galaxy S23', 1, 2, 759.00, 'haute'),
('TP-MBA-M2', 'MacBook Air M2', 2, 1, 1299.00, 'haute'),
('TP-DEL-XPS13', 'Dell XPS 13', 2, 6, 999.00, 'normale'),
('TP-IPD-AIR', 'iPad Air 5ème génération', 3, 1, 699.00, 'haute');

-- Index pour optimiser les performances
CREATE INDEX idx_produits_concurrents_collecte ON produits_concurrents(date_collecte DESC);
CREATE INDEX idx_historique_prix_date ON historique_prix(date_prix DESC);
CREATE INDEX idx_logs_collecte_site_date ON logs_collecte(id_site, date_debut DESC);

-- Vue pour les analyses de prix
CREATE VIEW vue_comparaison_prix AS
SELECT 
    pt.reference_interne,
    pt.nom_produit,
    pt.prix_vente_ttc as prix_techpulse,
    sc.nom_site,
    pc.prix_ttc as prix_concurrent,
    pc.en_promotion,
    pc.prix_promotion,
    pc.note_moyenne,
    pc.nombre_avis,
    pc.date_collecte,
    ROUND(((pc.prix_ttc - pt.prix_vente_ttc) / pt.prix_vente_ttc * 100), 2) as ecart_pourcentage
FROM produits_techpulse pt
JOIN produits_concurrents pc ON pt.id_produit = pc.id_produit_techpulse
JOIN sites_concurrents sc ON pc.id_site = sc.id_site
WHERE pc.date_collecte >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY pt.reference_interne, pc.date_collecte DESC;

-- Fonction pour nettoyer les anciennes données
CREATE OR REPLACE FUNCTION nettoyer_anciennes_donnees()
RETURNS void AS $$
BEGIN
    -- Supprimer les données de collecte de plus de 1 an
    DELETE FROM produits_concurrents 
    WHERE date_collecte < CURRENT_DATE - INTERVAL '1 year';
    
    -- Supprimer les logs de plus de 6 mois
    DELETE FROM logs_collecte 
    WHERE date_debut < CURRENT_DATE - INTERVAL '6 months';
    
    -- Compacter l'historique des prix (garder 1 prix par semaine pour les données anciennes)
    -- Cette fonction sera détaillée selon les besoins
    
    RAISE NOTICE 'Nettoyage terminé';
END;
$$ LANGUAGE plpgsql;