import json
import os
import shutil
import time

class Service:
    def __init__(self, id, type):
        self.id, self.type = id, type

class Employee:
    def __init__(self, id, first_name, last_name, join_date):
        self.id, self.first_name, self.last_name, self.join_date = id, first_name, last_name, join_date

class Customer(Employee):
    def __init__(self, id, first_name, last_name, join_date, address):
        Employee.__init__(self, id, first_name, last_name, join_date)
        self.address = address

class CheckingAccount:
    def __init__(self, id, customer_id, open_date, balance):
        self.id, self.customer_id, self.open_date, self.balance = id, customer_id, open_date, balance
        self.type_id, self.type_name = 1, 'CHECKING ACCOUNT'

    def show_balance(self):
        print(self.type_name)
        print(f'Current balance: ${self.balance}')

    def make_withdrawal(self, amount):
        if amount <= 0:
            raise ValueError('Please enter a valid withdrawal amount.')
        try:
            post_tran = self.balance - amount
            print(self.type_name)
            if post_tran < 0:
                print(f'Balance is currently ${self.balance}. Cannot withdraw more than that.')
            else:
                print(f'Balance before transaction: ${self.balance}')
                print(f'Transaction amount: ${amount}')
                print('... ...PROCESSING...')
                self.balance = post_tran
                time.sleep(3)
                print(f'Remaining balance: ${self.balance}')
        except:
            print('Please enter a valid withdrawal amount.')

    def make_deposit(self, amount):
        if amount <= 0:
            raise ValueError('Please enter a valid withdrawal amount.')
        try:
            print(self.type_name)
            print(f'Balance before transaction: ${self.balance}')
            print(f'Transaction amount: ${amount}')
            print('... ...PROCESSING...')
            self.balance += amount
            time.sleep(3)
            print(f'Remaining balance: ${self.balance}')
        except:
            print('Please enter a valid withdrawal amount.')

class SavingsAccount(CheckingAccount):
    def __init__(self, id, customer_id, open_date, balance):
        CheckingAccount.__init__(self, id, customer_id, open_date, balance)
        self.type_id, self.type_name, self.interest_rate = 2, 'SAVINGS ACCOUNT', 0.01

    def credit_interest(self):
        print(self.type_name)
        print(f'Balance before interest credit: ${self.balance}')
        print(f'Interest rate: {self.interest_rate * 100}%')
        print('... ...PROCESSING...')
        self.balance *= (1 + self.interest_rate)
        time.sleep(3)
        print(f'Remaining balance: ${self.balance}')

class CreditAccount(SavingsAccount):
    def __init__(self, id, customer_id, open_date, balance):
        SavingsAccount.__init__(self, id, customer_id, open_date, balance)
        self.type_id, self.type_name, self.interest_rate = 3, 'CREDIT ACCOUNT', 0.2

    def make_withdrawal(self, amount):
        print(f'Cannot withdraw from a {self.type_name}')

    def make_deposit(self, amount):
        print(f'Cannot deposit into a {self.type_name}')

    def make_payment(self, amount):
        if amount <= 0:
            raise ValueError('Please enter a valid withdrawal amount.')
        try:
            print(self.type_name)
            print(f'Balance before transaction: ${self.balance}')
            print(f'Transaction amount: ${amount}')
            if amount > self.balance:
                print(f'${amount} exceeds the current balance. A payment in the amount of the balance (${self.balance}) will be made.')
                self.balance = 0
            else:
                self.balance -= amount
            print('... ...PROCESSING...')
            time.sleep(3)
            print(f'Remaining balance: ${self.balance}')
        except:
            print('Please enter a valid withdrawal amount.')

class Bank:
    def __init__(self):
        self.initial_data_file, self.current_data_file = 'bank_database_INITIAL_STATE.json', 'bank_database.json'
        self.__mirror_database()

    def __mirror_database(self):
        if not os.path.exists(self.current_data_file):
            shutil.copy(self.initial_data_file, self.current_data_file)

        with open(self.current_data_file) as f:
            self.current_data = json.load(f)

        for i, service in enumerate(self.current_data['services'], start=1):
            globals()[f'service{i}'] = Service(service['id'], service['type'])

        for i, employee in enumerate(self.current_data['employees'], start=1):
            globals()[f'employee{i}'] = Employee(employee['id'], employee['first_name'], employee['last_name'],
                                                 employee['join_date'])

        for i, customer in enumerate(self.current_data['customers'], start=1):
            globals()[f'customer{i}'] = Customer(customer['id'], customer['first_name'], customer['last_name'],
                                                 customer['join_date'], customer['address'])

        for i, account in enumerate(self.current_data['accounts'], start=1):
            if account['type_id'] == 1:
                globals()[f'account{i}'] = CheckingAccount(account['id'], account['customer_id'], account['open_date'],
                                                           account['balance'])
            elif account['type_id'] == 2:
                globals()[f'account{i}'] = SavingsAccount(account['id'], account['customer_id'], account['open_date'],
                                                          account['balance'])
            elif account['type_id'] == 3:
                globals()[f'account{i}'] = CreditAccount(account['id'], account['customer_id'], account['open_date'],
                                                         account['balance'])
            else:
                print('Initial data is corrupt. Accounts may have not been properly initialized.')

    def __update_database(self):
        with open(self.current_data_file, 'w') as outfile:
            json.dump(self.current_data, outfile)

    def show_balance(self, account_id):
        globals()[f'account{account_id}'].show_balance()

    def make_deposit(self, account_id, amount):
        globals()[f'account{account_id}'].make_deposit(amount)
        self.current_data['accounts'][account_id - 1]['balance'] = globals()[f'account{account_id}'].balance
        self.__update_database()

    def make_withdrawal(self, account_id, amount):
        globals()[f'account{account_id}'].make_withdrawal(amount)
        self.current_data['accounts'][account_id - 1]['balance'] = globals()[f'account{account_id}'].balance
        self.__update_database()

    def credit_interest(self, account_id):
        globals()[f'account{account_id}'].credit_interest()
        self.current_data['accounts'][account_id - 1]['balance'] = globals()[f'account{account_id}'].balance
        self.__update_database()

    def make_payment(self, account_id, amount):
        globals()[f'account{account_id}'].make_payment(amount)
        self.current_data['accounts'][account_id - 1]['balance'] = globals()[f'account{account_id}'].balance
        self.__update_database()

    def reset_bank(self):
        """This will reset the current bank database to the initial state. Only use this to get a clean starting point."""
        os.remove(self.current_data_file)

        self.__mirror_database()

my_bank = Bank()