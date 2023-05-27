from arnold.getting_API_info import API_request
from arnold.ETL import Activities
from arnold.utils import find_location

from arnold.parameters import *

def main():

    #request = API_request()
    #request.getting_credentials()
    #request.getting_activity_data(url_list[0])

    #activity_process = Activities()
    #activity_process.top_activities()

    
    find_location()

if __name__ == "__main__":
    main()