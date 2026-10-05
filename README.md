# Projet 2 — Client Python vers HDFS via HttpFS

Projet du cours **Base de données massives avancées (030522)** — Groupe 6.

Un programme Python, exécuté sur l'ordinateur (hors des conteneurs), téléverse, liste et télécharge des fichiers dans un cluster Hadoop 3.3.6 conteneurisé. **Toutes les communications passent par la passerelle HttpFS** : le client n'accède jamais directement aux DataNodes.

## Image Docker publique

Image HttpFS publiée sur Docker Hub :

**https://hub.docker.com/r/2grene/hadoop-httpfs**

```powershell
docker pull 2grene/hadoop-httpfs:latest
```

Cette image est dérivée de `ous1/hadoop-cluster:latest` (image du cours). Elle enregistre le système de fichiers du conteneur HttpFS, mais pas les processus : le service HttpFS doit être démarré après le `docker run` (voir l'étape 3).

## Architecture

```
Ordinateur (client Python)
        |  HTTP, port 14000
        v
HttpFS (conteneur hadoop-httpfs)
        |  réseau Docker « hadoop »
        +--> NameNode (hadoop-master) : métadonnées
        +--> DataNodes (worker1 à worker5) : blocs de données
```

## Prérequis

- Docker Desktop
- Python 3 et pip
- Windows PowerShell (les commandes ci-dessous sont écrites pour PowerShell)
- Utiliser `curl.exe` et non `curl`, qui est un alias de `Invoke-WebRequest` dans PowerShell

## Mise en route

### 1. Démarrer le cluster Hadoop

```powershell
docker pull ous1/hadoop-cluster:latest
docker network create --driver=bridge hadoop
docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master ous1/hadoop-cluster:latest
1..5 | ForEach-Object { docker run -itd -p "$(8039+$_):8042" --net=hadoop --name hadoop-worker$_ --hostname hadoop-worker$_ ous1/hadoop-cluster:latest }
```

Entrer dans le master et démarrer les services Hadoop :

```powershell
docker exec -it hadoop-master bash
```

```bash
./start-hadoop.sh
jps
```

Le fichier `/usr/local/hadoop/etc/hadoop/workers` doit lister `hadoop-worker1` à `hadoop-worker5`.

### 2. Autoriser HttpFS à agir au nom de l'utilisateur HDFS (proxyuser)

Cette configuration se fait **sur hadoop-master**, pas dans le conteneur HttpFS. Sans elle, HttpFS répond `User: root is not allowed to impersonate root`.

Dans le conteneur `hadoop-master` :

```bash
cd /usr/local/hadoop/etc/hadoop/
sed -i 's#</configuration>#  <property>\n    <name>hadoop.proxyuser.root.hosts</name>\n    <value>*</value>\n  </property>\n  <property>\n    <name>hadoop.proxyuser.root.groups</name>\n    <value>*</value>\n  </property>\n</configuration>#' core-site.xml
hdfs --daemon stop namenode
hdfs --daemon start namenode
exit
```

Le joker `*` convient à un laboratoire protégé. En production, on restreindrait les hôtes et les groupes.

### 3. Lancer la passerelle HttpFS

```powershell
docker run -itd -p 14000:14000 --net=hadoop --name hadoop-httpfs --hostname hadoop-httpfs 2grene/hadoop-httpfs:latest
docker exec hadoop-httpfs httpfs.sh start
```

Vérifier que la passerelle répond depuis l'ordinateur :

```powershell
curl.exe "http://localhost:14000/webhdfs/v1/?op=LISTSTATUS&user.name=root"
```

### 4. Créer le répertoire de travail dans HDFS

```powershell
curl.exe -X PUT "http://localhost:14000/webhdfs/v1/projet2?op=MKDIRS&user.name=root"
```

Réponse attendue : `{"boolean":true}`

### 5. Installer les dépendances Python

```powershell
pip install -r requirements.txt
```

## Utilisation du client

```powershell
python httpfs_client.py upload test1.csv /projet2/test1.csv
python httpfs_client.py list /projet2
python httpfs_client.py download /projet2/test1.csv test1_copie.csv
```

| Option | Rôle |
|---|---|
| `--overwrite` | Autorise l'écrasement d'une destination existante (refusé par défaut) |
| `--gateway URL` | Adresse de la passerelle (défaut : `http://localhost:14000`) |

Variables d'environnement : `HTTPFS_GATEWAY` (adresse de la passerelle) et `HTTPFS_USER` (utilisateur HDFS, `root` par défaut).

Le client vérifie la taille du fichier côté serveur après chaque transfert, refuse d'écraser une destination existante et limite l'attente à 15 secondes si la passerelle est injoignable.

## Validation (T1 à T7)

| Test | Action | Résultat |
|---|---|---|
| T1 | Téléverser deux fichiers | Réussi : `test1.csv` (69 o) et `test2.txt` (97 o) dans `/projet2` |
| T2 | Lister le répertoire | Réussi : nom, type et taille affichés |
| T3 | Télécharger un fichier | Réussi : `test1_copie.csv` créé sans écraser l'original |
| T4 | Vérifier l'intégrité | Réussi : empreintes SHA256 identiques |
| T5 | Tester deux erreurs | Réussi : chemin introuvable et passerelle injoignable, messages distincts |
| T6 | Destination existante | Réussi : refus d'écrasement explicite |
| T7 | Trajet réseau | Réussi : `httpfs-audit.log` trace chaque opération |

Preuve du trajet réseau, dans le conteneur HttpFS :

```powershell
docker exec -it hadoop-httpfs bash
tail -30 /usr/local/hadoop/logs/httpfs-audit.log
```

## Contenu du dépôt

| Fichier | Description |
|---|---|
| `httpfs_client.py` | Client Python (upload, list, download) |
| `requirements.txt` | Dépendances Python (`requests`) |
| `test1.csv`, `test2.txt` | Fichiers de test non sensibles |
| `test1_copie.csv` | Copie téléchargée depuis HDFS |
| Compte rendu (`.docx`) | Rapport du projet |

## Références

- [Apache Hadoop 3.3.6 — HttpFS](https://hadoop.apache.org/docs/r3.3.6/hadoop-hdfs-httpfs/index.html)
- [Apache Hadoop 3.3.6 — HDFS Architecture](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Docker — Bridge network driver](https://docs.docker.com/engine/network/drivers/bridge/)
