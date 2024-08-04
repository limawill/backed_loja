from dynaconf import Dynaconf
import logging

settings = Dynaconf(settings_files=["settings.toml"])

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
