"""
Client Python pour un cluster Hadoop/HDFS via la passerelle HttpFS.

Toutes les communications passent exclusivement par HttpFS (API WebHDFS v1
exposée en HTTP), jamais directement vers les DataNodes. HttpFS relaie en
interne les echanges avec le NameNode (metadonnees) et les DataNodes
(contenu des blocs).

Projet 2 - UA1 - Relier un environnement Python a un ecosysteme Big Data
Hadoop conteneurise.

Dependances : requests (pip install requests)
"""

import argparse
import os
import sys
import requests

# ---------------------------------------------------------------------------
# Configuration (modifiable sans toucher au reste du code)
# ---------------------------------------------------------------------------
GATEWAY_URL = os.environ.get("HTTPFS_GATEWAY", "http://localhost:14000")
HDFS_USER = os.environ.get("HTTPFS_USER", "root")
TIMEOUT_SECONDS = 15  # attente limitee en cas de service indisponible


class HttpFsError(Exception):
    """Erreur applicative levee par le client HttpFS (reseau, HTTP, logique)."""


def _base_url(hdfs_path: str) -> str:
    """Construit l'URL WebHDFS pour un chemin HDFS donne."""
    if not hdfs_path.startswith("/"):
        hdfs_path = "/" + hdfs_path
    return f"{GATEWAY_URL}/webhdfs/v1{hdfs_path}"


def _check_gateway_reachable() -> None:
    """Verifie rapidement que la passerelle repond, sinon leve une erreur claire."""
    try:
        requests.get(
            f"{GATEWAY_URL}/webhdfs/v1/?op=LISTSTATUS&user.name={HDFS_USER}",
            timeout=TIMEOUT_SECONDS,
        )
    except requests.exceptions.ConnectionError as exc:
        raise HttpFsError(
            f"Impossible de joindre la passerelle HttpFS a {GATEWAY_URL}. "
            "Verifiez qu'elle est demarree et que le port 14000 est publie."
        ) from exc
    except requests.exceptions.Timeout as exc:
        raise HttpFsError(
            f"La passerelle HttpFS n'a pas repondu dans le delai de "
            f"{TIMEOUT_SECONDS} secondes."
        ) from exc


def path_exists(hdfs_path: str) -> bool:
    """Indique si un chemin existe deja dans HDFS (via GETFILESTATUS)."""
    url = _base_url(hdfs_path)
    params = {"op": "GETFILESTATUS", "user.name": HDFS_USER}
    resp = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
    return resp.status_code == 200


def list_directory(hdfs_path: str) -> list:
    """
    Liste le contenu d'un repertoire HDFS.

    Retourne une liste de dicts {nom, type, taille_octets}.
    Leve HttpFsError si le chemin n'existe pas ou si la passerelle est injoignable.
    """
    _check_gateway_reachable()
    url = _base_url(hdfs_path)
    params = {"op": "LISTSTATUS", "user.name": HDFS_USER}

    try:
        resp = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
    except requests.exceptions.RequestException as exc:
        raise HttpFsError(f"Erreur reseau lors du listing : {exc}") from exc

    if resp.status_code != 200:
        raise HttpFsError(
            f"Listing refuse (HTTP {resp.status_code}) pour '{hdfs_path}'. "
            f"Reponse du serveur : {resp.text}"
        )

    data = resp.json()
    entries = data.get("FileStatuses", {}).get("FileStatus", [])
    return [
        {
            "nom": e["pathSuffix"],
            "type": e["type"],  # "FILE" ou "DIRECTORY"
            "taille_octets": e["length"],
        }
        for e in entries
    ]


def upload_file(local_path: str, hdfs_path: str, overwrite: bool = False) -> None:
    """
    Televerse un fichier local vers HDFS via HttpFS.

    Refuse d'ecraser une destination existante si overwrite=False
    (protection contre les ecrasements involontaires).
    """
    _check_gateway_reachable()

    if not os.path.isfile(local_path):
        raise HttpFsError(f"Fichier local introuvable : '{local_path}'")

    if not overwrite and path_exists(hdfs_path):
        raise HttpFsError(
            f"Le chemin HDFS '{hdfs_path}' existe deja. "
            "Relancez avec overwrite=True pour l'ecraser explicitement."
        )

    url = _base_url(hdfs_path)
    params = {
        "op": "CREATE",
        "user.name": HDFS_USER,
        "overwrite": str(overwrite).lower(),
    }

    with open(local_path, "rb") as f:
        data = f.read()

    try:
        # Etape 1 : HttpFS repond par une redirection 307 vers l'URL d'ecriture
        # reelle (requests la suit automatiquement avec allow_redirects=True,
        # en conservant la methode PUT et le corps de la requete).
        resp = requests.put(
            url,
            params=params,
            data=data,
            headers={"Content-Type": "application/octet-stream"},
            allow_redirects=True,
            timeout=TIMEOUT_SECONDS,
        )
    except requests.exceptions.RequestException as exc:
        raise HttpFsError(f"Erreur reseau lors du televersement : {exc}") from exc

    if resp.status_code not in (200, 201):
        raise HttpFsError(
            f"Televersement refuse (HTTP {resp.status_code}) vers '{hdfs_path}'. "
            f"Reponse du serveur : {resp.text}"
        )

    # Confirmation reelle : on revérifie cote serveur que le fichier existe
    # bien et que sa taille correspond (pas de "faux succes").
    status = get_file_status(hdfs_path)
    taille_locale = os.path.getsize(local_path)
    if status["taille_octets"] != taille_locale:
        raise HttpFsError(
            f"Televersement incomplet : taille locale {taille_locale} octets, "
            f"taille HDFS {status['taille_octets']} octets."
        )


def download_file(hdfs_path: str, local_path: str, overwrite: bool = False) -> None:
    """
    Telecharge un fichier HDFS vers le disque local via HttpFS, sans le modifier.

    Refuse d'ecraser un fichier local existant si overwrite=False.
    """
    _check_gateway_reachable()

    if not overwrite and os.path.exists(local_path):
        raise HttpFsError(
            f"Le fichier local '{local_path}' existe deja. "
            "Choisissez un autre nom ou relancez avec overwrite=True."
        )

    if not path_exists(hdfs_path):
        raise HttpFsError(f"Chemin HDFS introuvable : '{hdfs_path}'")

    url = _base_url(hdfs_path)
    params = {"op": "OPEN", "user.name": HDFS_USER}

    try:
        resp = requests.get(
            url, params=params, allow_redirects=True, timeout=TIMEOUT_SECONDS
        )
    except requests.exceptions.RequestException as exc:
        raise HttpFsError(f"Erreur reseau lors du telechargement : {exc}") from exc

    if resp.status_code != 200:
        raise HttpFsError(
            f"Telechargement refuse (HTTP {resp.status_code}) pour '{hdfs_path}'. "
            f"Reponse du serveur : {resp.text}"
        )

    with open(local_path, "wb") as f:
        f.write(resp.content)

    # Verification d'integrite immediate (taille)
    status = get_file_status(hdfs_path)
    taille_locale = os.path.getsize(local_path)
    if status["taille_octets"] != taille_locale:
        raise HttpFsError(
            f"Telechargement incomplet : taille HDFS {status['taille_octets']} "
            f"octets, taille locale obtenue {taille_locale} octets."
        )


def get_file_status(hdfs_path: str) -> dict:
    """Recupere les metadonnees (type, taille) d'un chemin HDFS."""
    url = _base_url(hdfs_path)
    params = {"op": "GETFILESTATUS", "user.name": HDFS_USER}
    resp = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
    if resp.status_code != 200:
        raise HttpFsError(
            f"Impossible d'obtenir le statut de '{hdfs_path}' "
            f"(HTTP {resp.status_code}) : {resp.text}"
        )
    fs = resp.json()["FileStatus"]
    return {"type": fs["type"], "taille_octets": fs["length"]}


# ---------------------------------------------------------------------------
# Interface en ligne de commande
# ---------------------------------------------------------------------------
def _cmd_upload(args):
    upload_file(args.local, args.hdfs, overwrite=args.overwrite)
    print(f"OK : '{args.local}' televerse vers '{args.hdfs}'")


def _cmd_list(args):
    entries = list_directory(args.hdfs)
    if not entries:
        print(f"(repertoire vide : {args.hdfs})")
        return
    print(f"{'NOM':<35} {'TYPE':<10} TAILLE (octets)")
    for e in entries:
        print(f"{e['nom']:<35} {e['type']:<10} {e['taille_octets']}")


def _cmd_download(args):
    download_file(args.hdfs, args.local, overwrite=args.overwrite)
    print(f"OK : '{args.hdfs}' telecharge vers '{args.local}'")


def main():
    global GATEWAY_URL

    parser = argparse.ArgumentParser(
        description="Client HDFS via passerelle HttpFS (upload / list / download)."
    )
    parser.add_argument(
        "--gateway",
        default=None,
        help=f"URL de la passerelle HttpFS (defaut : {GATEWAY_URL})",
    )
    sub = parser.add_subparsers(dest="commande", required=True)

    p_up = sub.add_parser("upload", help="Televerser un fichier local vers HDFS")
    p_up.add_argument("local", help="Chemin du fichier local")
    p_up.add_argument("hdfs", help="Chemin de destination dans HDFS")
    p_up.add_argument("--overwrite", action="store_true")
    p_up.set_defaults(func=_cmd_upload)

    p_ls = sub.add_parser("list", help="Lister un repertoire HDFS")
    p_ls.add_argument("hdfs", help="Chemin du repertoire HDFS")
    p_ls.set_defaults(func=_cmd_list)

    p_dl = sub.add_parser("download", help="Telecharger un fichier HDFS")
    p_dl.add_argument("hdfs", help="Chemin du fichier dans HDFS")
    p_dl.add_argument("local", help="Chemin local de destination")
    p_dl.add_argument("--overwrite", action="store_true")
    p_dl.set_defaults(func=_cmd_download)

    args = parser.parse_args()

    if args.gateway:
        GATEWAY_URL = args.gateway

    try:
        args.func(args)
    except HttpFsError as exc:
        print(f"ERREUR : {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
