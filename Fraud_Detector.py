import pandas as pd 
import numpy as np 
from copy import deepcopy









#collect required information from historical data
def conclude_historical_Transactions(historical_data,risky_countries_list):
    


    ip_list=[]

    for index,ip in historical_data['IP_Address'].iteritems():
        ip_list.append(ip)

    client_data={}
    for index,client in historical_data['Customer_ID'].iteritems():
        if client not in  client_data:
            months_dict={x:0 for x in range(1,13)}
            for inner_index,sender in historical_data['Customer_ID'].iteritems():
                if client==sender:
                    t_month=int(historical_data['Transaction_Date'][inner_index].month)
                    if t_month in months_dict:
                        months_dict[t_month]+=1

                    else:
                        print(f'something is seriously wrong !!!')
                        return ValueError




            ben_ids=set()
            num_risky_list_country=0
            
            trans_amounts=[]


            for inner_index,sender in historical_data['Customer_ID'].iteritems():
                if client==sender:
                    trans_amounts.append(historical_data['Transaction_amount'][inner_index])
            trans_amounts.sort()
            highest_amount=max(trans_amounts)
            amounts_standard_deviation=np.std(highest_amount)
            

            for inner_index,sender in historical_data['Customer_ID'].iteritems():
                if client==sender:
                    transaction_amount=historical_data['Transaction_amount'][inner_index]
                    amount_condition=  transaction_amount >= (highest_amount + 2 * (amounts_standard_deviation))
                    if amount_condition :
                        historical_data["Fraud"][inner_index]='TRUE'
                       
            for inner_index,sender in historical_data['Customer_ID'].iteritems():
                if client==sender:
                    ben_ids.add(historical_data['Beneficiary_ID'][inner_index])
                    if historical_data['Beneficiary_Country'][inner_index] in risky_countries_list:
                        historical_data["Fraud"][inner_index]='TRUE'
                        num_risky_list_country+=1
                       

            client_data[client]=ben_ids,num_risky_list_country,highest_amount,amounts_standard_deviation,months_dict



    client_description_dict={}
    for client,c_data in client_data.items():
        client_description_dict[client]=set()
    chosen_clients=set()
    for client, description_set in   client_description_dict.items():
        if client not in chosen_clients:
            chosen_clients.add(client)
            for inner_index,inner_client in historical_data['Customer_ID'].iteritems():
                if client==inner_client:
                    client_description_dict[client].add(historical_data['Description'][inner_index])
    

    client_month_num_reversed={}
    for client,c_data in client_data.items():
        client_month_num_reversed[client]={x:0 for x in range(1,13)}


    for client,months_reverse in client_month_num_reversed.items():
        for inner_index,inner_client in historical_data['Customer_ID'].iteritems():
            if inner_client==client:
                month=historical_data['Transaction_Date'][inner_index].month
                reversed_status=historical_data['Reversed'][inner_index]
                if reversed_status=='Yes':
                    client_month_num_reversed[client][month]+=1



    client_ips={}
    for client,c_data in client_data.items():
        client_ips[client]=set()
    for index,client in historical_data['Customer_ID'].iteritems():
        for inner_index,inner_client in historical_data['Customer_ID'].iteritems():
            if client==inner_client:
                client_ips[client].add(historical_data['IP_Address'][inner_index])



    

    return client_data,historical_data,ip_list,client_ips,client_month_num_reversed,client_description_dict

def decide_test_data(test_data,client_data,risky_countries_list,ip_list,client_ips,client_month_num_reversed,client_description_dict):

    #Rule 4 number of the transaction
    client_months_transactions={}
    for client,v in client_data.items():
        client_months_transactions[client]={}
    for client,months_transactions in client_months_transactions.items():
        client_months_transactions[client]={x:0 for x in range(1,13)}



    #Rule 6 reversed transactions
    local_client_months_revesed={}
    for client,v in client_data.items():
        local_client_months_revesed[client]={}
    for client,months_reverses  in local_client_months_revesed.items():
        local_client_months_revesed[client]={x:0 for x in range(1,13)}


    
    for index,client in test_data['Customer_ID'].iteritems():
        tran_excuted=0
        if client in client_data:


            #Rule 4 continuation 
            #
            local_trans_month=int(test_data['Transaction_Date'][index].month)
            client_months_transactions[client][local_trans_month]+=1
            ben_ids,num_risky_list_country,highest_amount,amounts_standard_deviation,months_dict=client_data[client]
            highest_num_trans=0
            local_standard_deviation_list=[]
            for month,num_transactions in months_dict.items():
                if num_transactions>= highest_num_trans:
                    highest_num_trans=num_transactions
                local_standard_deviation_list.append(months_dict[month])

            local_standard_deviation=np.std(local_standard_deviation_list)
            this_month_num_transactions= client_months_transactions[client][local_trans_month]


            month_limit= highest_num_trans + (2*(local_standard_deviation))
            month_limit_condition= this_month_num_transactions >=month_limit
            if month_limit_condition:
                test_data["Fraud"][index]='TRUE'
                print(f'\n\n >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>customer transaction of index {index} is FRAUD because of the month limit condtion\n\n ')


            #Rule 1 Beneficiary Country
            #check if the Beneficiary Country is in the risky list, if so then the transaction is Fraud
            tran_country=test_data['Beneficiary_Country'][index]
            if tran_country in risky_countries_list:
                test_data["Fraud"][index]='TRUE'
            
            

            #Rule 2 Beneficiary ID
            #create a set of unique Beneficiary_IDs for each client in historical_data dataset (this step is done in function (conclude_historical_Transactions ()) 
            #check if the Beneficiary_ID of the transaction in the test_dat dataset of each client in the prviously made set of uniqe Beneficiary_IDs 
            #if not then the transaction is Fraud

            ben_id=test_data['Beneficiary_ID'][index]
            if ben_id not in ben_ids:
                test_data["Fraud"][index]='TRUE'
            

            #Rule 3 amount of the transaction
            #create a set of amounts transfered by each client in the historical date (this step is done in function (conclude_historical_Transactions ()) 
            #find the highest amount transacted by each clientin the historical_data dataset (this step is done in function (conclude_historical_Transactions ()) 
            #find the standard deviation of the list of the amounts trasacted by ther client in the historical_data dataset (this step is done in function (conclude_historical_Transactions ())
            
            transaction_amount=test_data['Transaction_amount'][index]
            #add 2 times the standard deviation to the highest amount and compare the result to the transaction amount in the test_data dataset
            amount_condition=  transaction_amount >= (highest_amount + 2 * (amounts_standard_deviation))
            #if higher then the transaction is Fraud
            if amount_condition :
                test_data["Fraud"][index]='TRUE'
            

            #Rule 5 IP Address
            #create a set of unique IP_Addresses used by  each client in historical_data dataset (this step is done in function (conclude_historical_Transactions ()) 
            #Check if the ip of this transaction has been used by any other client than the current client in the historical_data dataset
            #if so then the transaction is Fraud

            cust_ip=test_data['IP_Address'][index]
            temp_ips_set=deepcopy(ip_list)
            cust_used_ips=deepcopy(client_ips[client])
            for used_ip in cust_used_ips:
                temp_ips_set.remove(used_ip)
            if cust_ip  in ip_list:
                test_data["Fraud"][index]='TRUE'
                print(f'\n\n >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>customer transaction of index {index} is FRAUD because of IP ADDRESS!!!!\n\n ')

            


            #Rule 6 continuation
            #Create a a dictionary of the monthy number of reversed transactrions of each client in the historical_data dataset (this step is done in function (conclude_historical_Transactions ()) 
            #check if the number of reversed transactions in the last month (test_data dataset) is higher than 
            #the highest monthly number of reversed in the client history and add the this value to 2 times the standard deviation
            #of historical monthly number of reversed transactions 
            #if if hgher then the transaction is Fraud

            highest_num_reversed=0
            reversed_months_of_client=client_month_num_reversed[client]
            reversed_months_list=[]
            for month,num_reversed in reversed_months_of_client.items():
                if num_reversed>= highest_num_reversed:
                    highest_num_reversed=num_reversed
                reversed_months_list.append(num_reversed)
            reversed_std=np.std(reversed_months_list)
           
            meant_month=int(test_data['Transaction_Date'][index].month)
            rev_status=test_data['Reversed'][index]
            if rev_status=='Yes':
                local_client_months_revesed[client][meant_month]+=1

            
            this_month_num_reversed=local_client_months_revesed[client][meant_month]
            month_reversed= highest_num_reversed + (2*(reversed_std))
            month_reversed_condition= this_month_num_reversed >=month_reversed
            if month_reversed_condition:
                test_data["Fraud"][index]='TRUE'
                print(f'\n\n >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>customer transaction of index {index} is FRAUD because REVERSED$$$$$ condtion\n\n ')

            



            # Rule 7 Description of the transaction
            #create a dictionary of client and description for each client in the historical_data dataset (this step is done in function (conclude_historical_Transactions ()) 
            #find the status for the current client historical transactions description which can be either all_yes or all_no or mixed
            #compare the description status of the current transaction to the value obtained in the last step
            #if its old_status == "all_yes" and the current transaction status is = "No" then the transaction is fraud 
            # the same if  old_status == "all_no" and the current transaction description status is = "Yes"

            client_historical_descriprtion=client_description_dict[client]
            all_yes=True
            all_no=True
            for old_desc in client_historical_descriprtion:
                if old_desc=='Yes':
                    all_no=False
                else:
                    all_yes=False
            if all_yes and (not all_no):
                old_des_status="all_yes"
            elif all_no and (not all_yes):
                old_des_status="all_no"
            else:
                old_des_status="mixed"
            
           

            for inner_index,inner_client in test_data['Customer_ID'].iteritems():
                if client==inner_client:
                    client_new_descriprtion=test_data['Description'][inner_index]
                    if client_new_descriprtion=='Yes':
                        if old_des_status=="all_no":
                            test_data["Fraud"][inner_index]='TRUE'
                            print(f'\n\n >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>customer transaction of index {index} is FRAUD because ><><><>><   DESCRIPTION    ><><><><><$$$$$ condtion\n\n ')
                
                    elif client_new_descriprtion=='No':
                        if old_des_status=="all_yes":
                            test_data["Fraud"][inner_index]='TRUE'
                            print(f'\n\n >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>customer transaction of index {index} is FRAUD because ><><><>><   DESCRIPTION    ><><><><><$$$$$ condtion\n\n ')
                



            
            

            



       
    return test_data

                

                
  
def main():
    try:
        historical_data_path=input('Please Enter absolute path for Historical data dataset: \n')
        test_data_path=input('Please Enter absolute path for Test data dataset: \n')

        historical_data=pd.read_excel(historical_data_path,engine='openpyxl')
        test_data=pd.read_excel(fr'{test_data_path}',engine='openpyxl')

        risky_countries_list=['Albania', 'Barbados', 'Burkina' 'Faso', 'Cambodia', 'Cayman' 'Islands', 'Haiti','Jamaica','Jordan',
            'Mali', 'Malta', 'Morocco', 'Myanmar', 'Nicaragua', 'Pakistan', 'Panama' , 'Philippines', 'Senegal',
            'South Sudan', 'Syria', 'Turkey', 'Uganda', 'Yemen','Zimbabwe']
        historical_data.insert(2, "Fraud",'FALSE' ) 

        historical_data.columns=['Customer_ID', 'Gender', 'Fraud', 'Customer_IBAN', 'Costumer_Country',
            'Transaction_amount', 'Transaction_Date', 'Description', 'IP_Address',
            'Beneficiary_ID', 'Beneficiary_Country', 'Reversed']
        print()


        test_data.insert(2, "Fraud",'FALSE' ) 
        test_data.columns=['Customer_ID', 'Gender', 'Fraud', 'Customer_IBAN', 'Costumer_Country',
            'Transaction_amount', 'Transaction_Date', 'Description', 'IP_Address',
            'Beneficiary_ID', 'Beneficiary_Country', 'Reversed']

        client_data,historical_data,ip_list,client_ips,client_month_num_reversed,client_description_dict=conclude_historical_Transactions(historical_data,risky_countries_list)
        concluded_test_data=decide_test_data(test_data,client_data,risky_countries_list,ip_list,client_ips,client_month_num_reversed,client_description_dict)
        

        hist_name=input('Please Enter Name desired to name the  concluded  Historical_data  dataset: \n')
        new_historical_path=input('Please Enter absolute path to save concluded  Historical_data  dataset: \n')
        hist_full_path=new_historical_path+'/'+hist_name+'.xlsx'

        test_name=input('Please Enter Name desired to name the  concluded  Test_data  dataset: \n')
        new_test_path=input('Please Enter absolute path to save concluded  Test_data dataset: \n')
        test_full_path=new_test_path+'/'+test_name+'.xlsx'

        historical_data.to_excel(hist_full_path, engine='xlsxwriter')
        concluded_test_data.to_excel(test_full_path, engine='xlsxwriter')

    except:
        raise


# /Users/aalmohamad/Documents/software_Engineering/ENIKA_TAFCIU (2)/Historical_data.xlsx
# /Users/aalmohamad/Documents/Eni_FD/Test_data.xlsx

if __name__ == "__main__":
   main()
