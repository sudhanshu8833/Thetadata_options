from sql.models import *

from datetime import datetime
import logging
import pandas as pd
import time as tim
from pytz import timezone
import traceback
from datetime import time, datetime
import yfinance as yf
logging.basicConfig(
    filename='log/dev.log',
    level=logging.DEBUG,  # You can adjust the log level as needed
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Base_strategy():

    def __init__(self, session):
        self.session = session
        self.parameters=session.query(Admin).all()[0]
        self.login()

    def download(self):
        
        df=yf.download(self.parameters.symbol,period='6mo',interval='5m')
        return df

    def main(self):
        df=self.download()


    def login(self):
        pass

    def market_order(self):
        pass

    def run(self):
        try:
            self.main()
        except Exception:
            logger.error(traceback.format_exc())


if __name__=="__main__":
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    val=Base_strategy(session)
    val.run()

