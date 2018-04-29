import pandas as pd
import numpy as np
import requests
import string
from bs4 import BeautifulSoup
import csv
import threading
import timeit
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count

url_df = pd.read_csv('fight_urls.csv')
url_matrix = url_df.as_matrix()
dict_dates = {}
for row in url_matrix:
    dict_dates[row[0]] = row[1]

def resultStats(result):
    fight_dict = {}
    fighters = result.find_all('div', {'class': 'b-fight-details__person'})
    try:
        fight_dict['fighter_a'] = fighters[0].find('a')['href'].split('/')[-1]
    except:
        name = fighters[0].find('span').text.strip()
        fight_dict['fighter_a'] = name.split(' ')[0].lower() + '_' + name.split(' ')[1].lower()
    try:
        fight_dict['fighter_b'] = fighters[1].find('a')['href'].split('/')[-1]
    except:
        name = fighters[1].find('span').text.strip()
        fight_dict['fighter_b'] = name.split(' ')[0].lower() + '_' + name.split(' ')[1].lower()
    fight_dict['fighter_a_result'] = fighters[0].find('i').text.strip()
    fight_dict['fighter_b_result'] = fighters[1].find('i').text.strip()
    return fight_dict

def entireFightStats(soup, fight_dict):
    overall_stats = soup.find_all('section', {'class':'b-fight-details__section js-fight-section'})[1].find_all('tr')[1].find_all('td')
    top = 'fighter_a'
    bottom = 'fighter_b'
    top_ind = 2
    bottom_ind = 5
    fight_dict['total' + '_' + top + '_'+ 'kd'] = overall_stats[1].text.split('\n')[top_ind].strip()
    fight_dict['total' + '_' + bottom + '_'+ 'kd'] = overall_stats[1].text.split('\n')[bottom_ind].strip()
    fight_dict['total' + '_' + top + '_'+ 'sub_att'] = overall_stats[7].text.split('\n')[top_ind].strip()
    fight_dict['total' + '_' + bottom + '_'+ 'sub_att'] = overall_stats[7].text.split('\n')[bottom_ind].strip()
    fight_dict['total' + '_' + top + '_'+ 'passes'] = overall_stats[8].text.split('\n')[top_ind].strip()
    fight_dict['total' + '_' + bottom + '_'+ 'passes'] = overall_stats[8].text.split('\n')[bottom_ind].strip()
    fight_dict['total' + '_' + top + '_'+ 'reverses'] = overall_stats[9].text.split('\n')[top_ind].strip()
    fight_dict['total' + '_' + bottom + '_'+ 'reverses'] = overall_stats[9].text.split('\n')[bottom_ind].strip()
    fight_dict['total' + '_' + top + '_'+ 'ssl'] = overall_stats[2].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'ssa'] = overall_stats[2].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'ssl']  = overall_stats[2].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'ssa']  = overall_stats[2].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + top + '_'+ 'tsl'] = overall_stats[4].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'tsa'] = overall_stats[4].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'tsl']  = overall_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'tsa']  = overall_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + top + '_'+ 'tdl'] = overall_stats[5].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'tda'] = overall_stats[5].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'tdl']  = overall_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'tda']  = overall_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    return fight_dict

def entireSigStats(soup, fight_dict):
    sig_stats = soup.find_all('tbody', {'class':'b-fight-details__table-body'})[2].find('tr').find_all('td')
    top = 'fighter_a'
    bottom = 'fighter_b'
    top_ind = 2
    bottom_ind = 5
    fight_dict['total' + '_' + top + '_'+ 'hsl'] = sig_stats[3].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'hsa'] = sig_stats[3].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'hsl'] = sig_stats[3].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'hsa'] = sig_stats[3].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + top + '_'+ 'bsl'] = sig_stats[4].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'bsa'] = sig_stats[4].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'bsl'] = sig_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'bsa'] = sig_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + top + '_'+ 'lsl'] = sig_stats[5].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'lsa'] = sig_stats[5].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'lsl'] = sig_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'lsa'] = sig_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + top + '_'+ 'dsl'] = sig_stats[6].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'dsa'] = sig_stats[6].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'dsl'] = sig_stats[6].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'dsa'] = sig_stats[6].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + top + '_'+ 'csl'] = sig_stats[7].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'csa'] = sig_stats[7].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'csl'] = sig_stats[7].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'csa'] = sig_stats[7].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + top + '_'+ 'gsl'] = sig_stats[8].text.split('\n')[top_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + top + '_'+ 'gsa'] = sig_stats[8].text.split('\n')[top_ind].strip().split(' ')[-1]
    fight_dict['total' + '_' + bottom + '_'+ 'gsl'] = sig_stats[8].text.split('\n')[bottom_ind].strip().split(' ')[0]
    fight_dict['total' + '_' + bottom + '_'+ 'gsa'] = sig_stats[8].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    return fight_dict

def summaryStats(summary, fight_dict):
    fight_dict['weight_class'] = summary.find('i').text.strip()
    elements = summary.find_all('i')[3:]
    fight_dict['method'] = elements[0].text.strip()
    fight_dict['round'] = elements[1].text.strip()[-1]
    fight_dict['time'] = elements[3].text.strip().split(' ')[-1]
    fight_dict['num_rounds'] = elements[5].text.split('Rnd')[0][-2]
    fight_dict['referee'] = elements[7].text.split('\n')[-3].strip()
    try:
        fight_dict['score1'] = elements[11].text.split('\n')[-2].strip()
        fight_dict['score2'] = elements[12].text.split('\n')[-2].strip()
        fight_dict['score3'] = elements[13].text.split('\n')[-2].strip()
    except:
        pass
    return fight_dict

def totalStats(totals, fight_dict):
    top = 'fighter_a'
    bottom = 'fighter_b'
    rounds = totals.find('tbody').find_all('tr')
    top_ind = 2
    bottom_ind = 5
    for i, Round in enumerate(rounds):
        r = str(i + 1)
        round_stats = Round.find_all('td')
        fight_dict[r+ '_'+ top + '_'+ 'kd'] = round_stats[1].text.split('\n')[top_ind].strip()
        fight_dict[r+ '_'+ bottom + '_'+ 'kd'] = round_stats[1].text.split('\n')[bottom_ind].strip()
        fight_dict[r+ '_'+ top + '_'+ 'ssl'] = round_stats[2].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r+ '_'+ top + '_'+ 'ssa'] = round_stats[2].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r+ '_'+ bottom + '_'+ 'ssl'] = round_stats[2].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r+ '_'+ bottom + '_'+ 'ssa'] = round_stats[2].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r+ '_'+ top + '_'+ 'tsl'] = round_stats[4].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r+ '_'+ top + '_'+ 'tsa'] = round_stats[4].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r+ '_'+ bottom + '_'+ 'tsl'] = round_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r+ '_'+ bottom + '_'+ 'tsa'] = round_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r+ '_'+ top + '_'+ 'tdl'] = round_stats[5].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r+ '_'+ top + '_'+ 'tda'] = round_stats[5].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r+ '_'+ bottom + '_'+ 'tdl'] = round_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r+ '_'+ bottom + '_'+ 'tda'] = round_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r+ '_'+ top + '_'+ 'sub_att'] = round_stats[7].text.split('\n')[top_ind].strip()
        fight_dict[r+ '_'+ bottom + '_'+ 'sub_att'] = round_stats[7].text.split('\n')[bottom_ind].strip()
        fight_dict[r+ '_'+ top + '_'+ 'passes'] = round_stats[8].text.split('\n')[top_ind].strip()
        fight_dict[r+ '_'+ bottom + '_'+ 'passes'] = round_stats[8].text.split('\n')[bottom_ind].strip()
        fight_dict[r+ '_'+ top + '_'+ 'reverses'] = round_stats[9].text.split('\n')[top_ind].strip()
        fight_dict[r+ '_'+ bottom + '_'+ 'reverses'] = round_stats[9].text.split('\n')[bottom_ind].strip()
    return fight_dict

def significantStrikes(sig_strikes, fight_dict):
    top = 'fighter_a'
    bottom = 'fighter_b'
    rounds = sig_strikes.find('tbody').find_all('tr')
    top_ind = 2
    bottom_ind = 5
    for i, Round in enumerate(rounds):
        r = str(i+1)
        round_stats = Round.find_all('td')
        fight_dict[r + '_' + top + '_' + 'hsl'] = round_stats[3].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r + '_' + top + '_' + 'hsa'] = round_stats[3].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + bottom + '_' + 'hsl'] = round_stats[3].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r + '_' + bottom + '_' + 'hsa'] = round_stats[3].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + top + '_' + 'bsl'] = round_stats[4].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r + '_' + top + '_' + 'bsa'] = round_stats[4].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + bottom + '_' + 'bsl'] = round_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r + '_' + bottom + '_' + 'bsa'] = round_stats[4].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + top + '_' + 'lsl'] = round_stats[5].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r + '_' + top + '_' + 'lsa'] = round_stats[5].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + bottom + '_' + 'lsl'] = round_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r + '_' + bottom + '_' + 'lsa'] = round_stats[5].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + top + '_' + 'dsl'] = round_stats[6].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r + '_' + top + '_' + 'dsa'] = round_stats[6].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + bottom + '_' + 'dsl'] = round_stats[6].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r + '_' + bottom + '_' + 'dsa'] = round_stats[6].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + top + '_' + 'csl'] = round_stats[7].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r + '_' + top + '_' + 'csa'] = round_stats[7].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + bottom + '_' + 'csl'] = round_stats[7].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r + '_' + bottom + '_' + 'csa'] = round_stats[7].text.split('\n')[bottom_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + top + '_' + 'gsl'] = round_stats[8].text.split('\n')[top_ind].strip().split(' ')[0]
        fight_dict[r + '_' + top + '_' + 'gsa'] = round_stats[8].text.split('\n')[top_ind].strip().split(' ')[-1]
        fight_dict[r + '_' + bottom + '_' + 'gsl'] = round_stats[8].text.split('\n')[bottom_ind].strip().split(' ')[0]
        fight_dict[r + '_' + bottom + '_' + 'gsa'] = round_stats[8].text.split('\n')[bottom_ind].strip().split(' ')[-1]
    return fight_dict

def Driver(url):
    #takes in a fight url and returns a dictionary of ALL the stats from the fight.
    #this includes round by round stats as well as overall totals
    fight_url = 'http://www.fightmetric.com/fight-details/{}'.format(url)
    response = requests.get(fight_url)
    soup = BeautifulSoup(response.content, 'lxml')
    result = soup.find('div', {'class': 'b-fight-details__persons clearfix'})
    fight_dict = resultStats(result)
    summary = soup.find('div', {'class':'b-fight-details__fight'})
    fight_dict = summaryStats(summary, fight_dict)
    fight_dict['date'] = dict_dates[url]
    try:
        fight_dict = entireFightStats(soup, fight_dict)
        fight_dict = entireSigStats(soup, fight_dict)
        totals, sig_strikes = soup.find_all('table', {'class': 'b-fight-details__table js-fight-table'})
        fight_dict = totalStats(totals, fight_dict)
        fight_dict = significantStrikes(sig_strikes, fight_dict)
    except:
        pass

    return fight_dict
if __name__ == "__main__":
    fileName = 'only_urls.csv'
    pool = Pool(cpu_count() * 2)
    with open(fileName, "rb") as f:
        results = pool.map(Driver, f)
    with open('Output.csv', 'ab') as f:
        writeFile = csv.writer(f)
        for result in results:
            writeFile.writerow(result)
