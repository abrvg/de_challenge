import csv
from datetime import datetime, timezone
import pandas as pd


def check_csv_empty(filename: str) -> bool:
    """
    Check if a csv data is empty. It can contain the header.

    Arg:
        filename: Path to the csv.
    """
    try:
        with open(filename, 'r', newline='') as f:
            reader = csv.reader(f)
            next(reader, None) # skip header (if exists)
            return all(not row for row in reader) # check if all rows are empty, return true if yes
        
    except Exception as e:
        print(f"Error ocurred: {e}")
        return False

    

def create_csv(file_name: str, headers: list[str]) -> None:
    """
    Create an empty csv file if not exist and write only the headers.

    Arg:
        filename: Name of the csv file to create
        headers: A list with the names of the headers

    """

    try:
        with open(file_name, 'a', newline='') as f:
            writer = csv.writer(f)

            #Check if the file is empty (no header written yet)
            if f.tell() == 0:
                writer.writerow(headers)
                print(f"CSV file '{file_name}' and headers added")
    
    except Exception as e:
        print(f"Error ocurred: {e}")




def etl_users(input_csv_path:str, output_csv_path:str) -> None:
    """
    This function reads the user_id from the input user csv file and adds them 
    to the user output file. It will only add users that do not exist

    Arg:
        input_csv_path (str): path of the source csv file
        output_csv_path (str): path for the target csv file
    """

    try:
    
        # create a set of all the existing users stored in the output users csv file
        # no memory issues, a set can contain hundred of millions records and can read or append in senconds (all user can be stored with no issue)
        # With the time the numbers of users wouldn't change too much, so the DAG for this function could run not daily
        existing_user = set()
        with open(output_csv_path, 'r') as target:
            reader = csv.reader(target)
            for row in reader:
                username = row[0]
                existing_user.add(username)


    # Read the input csv
    # If the user does not exist in the output csv, add it
        with open(input_csv_path, 'r') as source, open(output_csv_path, 'a') as target:
            reader = csv.reader(source)
            writer = csv.writer(target)
        
            for row in reader:

                # skip empty rows
                if not row:
                    continue
                
                username = row[0]
                if username not in existing_user:
                    timestamp = datetime.now(timezone.utc)
                    row.append(timestamp)
                    writer.writerow(row)
                    existing_user.add(username)
                    #print(f"Added user: {username}, created at: {timestamp}" )


    except Exception as e:
        print("Error ocurred: {e}")


def etl_events(input_csv_path:str, output_csv_path:str) -> None:
    """
    Function to populate the event table based on two criteries:
        If the Event table is empty just transfer directly
        Else only extract the rows generated during the day (since this script runs on a daily batch)

    Args:
        input_csv_path (str): Path to the input csv file
        output_csv_path (str): Path to the output csv file
    """

    try:
        #check if the out_csv is empty
        output_is_empty = check_csv_empty(output_csv_path)

        with open(input_csv_path, 'r') as source, open(output_csv_path, 'a') as target:
            reader = csv.reader(source)
            writer = csv.writer(target)

            next(reader, None) # skip header

            format_data = "%Y-%m-%d %H:%M:%S.%f"
            today = datetime.now(timezone.utc).date()

            for row in reader:
                timestamp = row[1]
                timestamp_date = datetime.strptime(timestamp, format_data).date()

                if output_is_empty or timestamp_date == today():
                    writer.writerow(row)

    
    except Exception as e:
        print(f"Error ocurred: {e}")



def etl_transactions(input_csv_path: str, output_csv_path:str, is_deposit:bool) -> None:
    """
    Insert transactions from deposit or withdrawal into the Transactions.csv

    Args:
        input_csv_path (str): Path to the input csv file
        output_csv_path (str): Path to the output csv file
        is_deposit (bool): True or False
    """
    try:
        columns = ["id", "event_timestamp", "user_id", "amount", "currency", "tx_status"]
        
        source_df = pd.read_csv(input_csv_path, usecols=columns)

        # type of transaction
        transaction_type = 'deposit' if is_deposit else 'withdrawal'
        source_df["transaction_type"] = transaction_type
        
        # ids are even for deposit and odd for withdrawal
        source_df["id"] = 2*source_df["id"] if is_deposit else 2*source_df["id"] - 1
        
        # check the existing id in the output file
        existing_id_df = pd.read_csv(output_csv_path, usecols=["id"])
        
        # Find new rows where 'id' in the source is not in the output
        new_rows = source_df[~source_df["id"].isin(existing_id_df["id"])].copy()
        
        #Drop null
        new_rows.dropna(inplace=True)
    
        # append the new rows
        new_rows.to_csv(output_csv_path, mode='a', index=False, header=False)

        
        
    except Exception as e:
        print(f"Error ocurred: {e}")



if __name__ == '__main__':
    
    #INPUT FILES:
    user_input_file = "input_data/user_id_sample_data.csv"
    event_input_file = "input_data/event_sample_data.csv"
    deposit_input_data = "input_data/deposit_sample_data.csv"
    withdrawal_input_data = "input_data/withdrawals_sample_data.csv"

    # OUTPUT FILES
    user_file = "output_data/Users.csv"
    event_file = "output_data/Events.csv"
    transaction_file = "output_data/Transactions.csv"

    # Create output csv if not exists (destination files)
    user_headers = ["user_id", "created_at"]
    create_csv(user_file, user_headers)

    event_headers = ["id","event_timestamp","user_id","event_name"]
    create_csv(event_file, event_headers)

    transactions_headers = ["id", "event_timestamp", "user_id", "amount", "currency", "tx_status", "transaction_type"]
    create_csv(transaction_file, transactions_headers)

    
    
    # RUN ETL
    etl_users(user_input_file, user_file)
    etl_events(event_input_file, event_file)
    etl_transactions(deposit_input_data, transaction_file, is_deposit=True)
    etl_transactions(withdrawal_input_data, transaction_file, is_deposit=False)
