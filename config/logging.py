import logging

# logger setup
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()
handler = logging.FileHandler("../logs/assignment.log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
