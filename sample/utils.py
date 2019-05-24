import pandas as pd
import sys
import snow.utils as sf
from itertools import cycle
import datetime

""" This script needs to:
    1. Ingest samples and convert them to emails
    2. Assign a user attribute of 'pushed-{x}'
    3. Evenly distribute samples. """

def ids_to_emails(cohort_file,remove_co_transactors=True):
    """ Ingests a specified file name with userIds; outputs a list of emails """
    emails = sf.from_snow(role='all_data_viewer'
                            ,wh='load_wh'
                            ,db='all_data'
                            ,q_kind='emails')

    try:
        error = 'Error converting uids to emails'
        cohort = pd.read_csv('input/'+str(cohort_file))

        ## Prep the dataframes for merge.
        try: 
            cohort = cohort.rename(index=str, columns={"ID":"id"})
            emails.id = emails.id.apply(lambda x:int(x))
            cohort.id = cohort.id.apply(lambda x:int(x))
        except: print('Renaming failed')

        error = 'Email-cohort merge failed'
        e_cohort = emails.merge(cohort)
        error = """Error: Users in the sample that are not in the returned
                    data from postgres"""
        assert cohort.shape[0] == e_cohort.shape[0]
    except:
        print("""\n-- {} --""".format(error))
        return emails, cohort
    
    if remove_co_transactors:
        cities = pd.DataFrame(sf.from_snow(role='all_data_viewer'
                                   ,wh='load_wh'
                                   ,db='all_data'
                                   ,schema='postgres'
                                   ,q_kind='user_cities'
                                   ,to_df=False)
                                   ,columns=['id','email','city'])
        cities['n'] = cities.groupby('id')['id'].transform('size')
        co_transactors = cities[cities.n>1].email.values

    return set(e_cohort.email.values) - set(co_transactors)


def sampler(emails,n_samples,cohort_name):
    """Ingests an email list with some n of samples...
    Returns a dataframe of ['email','sample'], and a list of individual dataframes'.
    """
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    interactions = sf.from_snow(role='all_data_viewer'
                                ,wh='load_wh'
                                ,db='all_data'
                                ,q_kind='interactions')
    
    filtered = interactions[interactions.email.isin(emails)]
    sortd = filtered.sort_values(by='date',ascending=False)
    total = sortd.drop_duplicates(subset='email')

    subsize = int(total.shape[0] / n_samples)
    remainder = total.shape[0] % subsize
    labels = list(range(n_samples))

    column_name = cohort_name+'_'+date
    total[column_name] = subsize*labels + labels[:remainder]

    return total, cohort_name


def samples_to_csv(df,cohort_name):
    """ Ingests the output of sampler, outputs csvs for each sample """
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    column_name = cohort_name+'_'+date

    samples = [df[df[column_name]==i].drop(column_name,axis=1) 
                    for i in df[column_name].unique()]

    for i,sample in enumerate(samples):
        output = pd.DataFrame(sample.email.values,columns=['email'])
        output.to_csv('output/{}.csv'.format(cohort_name+'_'+str(i)),index=False,header=False)
    