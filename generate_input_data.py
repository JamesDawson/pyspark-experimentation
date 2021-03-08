# %%
from faker import Faker
import pandas as pd

# %%
faker = Faker(local='en-GB')

# %%
# Fundamental constants
NUMBER_OF_PEOPLE =1000

# %%
faker.city()
# %%
class FakePerson ():

    def __init__(self):

        self.person_id = faker.unique.random_int(max=10*NUMBER_OF_PEOPLE)
        
        if faker.boolean(chance_of_getting_true=50):
            self.title = faker.prefix_female()
            self.first_name = faker.first_name_female()
        else:
            self.title = faker.prefix_male()
            self.first_name = faker.first_name_male()
       
        self.last_name = faker.last_name()

        self.street_address = faker.street_address().replace("\n", " ")
        self.city = faker.city()
        self.postcode = faker.postcode()

        self.phone_number = faker.phone_number()
        self.email = faker.email()

        self.job = faker.job()

        self.date_of_birth = faker.date_of_birth(minimum_age=18, maximum_age=75)

        self.last_login = faker.past_datetime(start_date='-30d')
        self.last_login_client = faker.user_agent()

        self.bank_balance = faker.








