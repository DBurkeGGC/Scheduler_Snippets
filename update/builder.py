from src.common.database import Database
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import requests as rs
import datetime as dt
import aiohttp
import asyncio
import json
import time
import re


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


def init_db():
    Database.initialize()


class Builder:

    def __init__(self):
        self.base_url = 'https://ggc.gabest.usg.edu/pls/B400/'
        self.terms = None
        self.prefixes = None
        self.subj_str = None
        self.raw_courses = None
        self.raw_sections = None
        self.courses = []
        self.sections = []
        self.times = []
        self.ratings = []
        self.log = []
        self.retry_delay = 5
        self.fatal_error = False

    # Scrapes current term codes
    def scrape_terms(self):
        try:
            data = rs.get(self.base_url + 'bwckschd.p_disp_dyn_sched').text
            soup = BeautifulSoup(data, 'lxml')
            temp = soup.select('.dedefault option')
            self.terms = []
            for option in temp:
                if '(View only)' not in option.get_text():
                    if option.get_text() != 'None':
                        self.terms.append(option['value'])
        except:
            self.log.append(str(dt.datetime.today()) + ' ERROR: PARSE FAILED; SCRAPE_TERMS()')
            self.fatal_error = True

    # Compiles valid prefixes from current semester(s)
    async def scrape_prefixes(self, retries=3):
        try:
            url = self.base_url + 'bwckctlg.p_disp_cat_term_date?call_proc_in=bwckctlg.p_disp_dyn_ctlg&cat_term_in={}'
            tasks = []
            async with ClientSession() as session:
                for term in self.terms:
                    tasks.append(asyncio.ensure_future(fetch(url.format(term), session)))
                prefix_html = await asyncio.gather(*tasks)
                self.prefixes = []
                self.subj_str = None
                for page in prefix_html:
                    soup = BeautifulSoup(page, 'lxml')
                    temp = soup.select('#subj_id option')
                    for item in temp:
                        if item['value'] not in self.prefixes:
                            self.prefixes.append(item['value'])
                self.subj_str = '&sel_subj='.join(str(prefix) for prefix in self.prefixes)
        except ValueError:
            retries -= 1
            self.log.append(str(dt.datetime.today()) + ' ERROR: VALUE ERROR ENCOUNTERED; SCRAPE_PREFIXES(); '
                            'RETRYING; ' + str(retries) + ' TRIES REMAINING')
            if retries != 0:
                await asyncio.sleep(self.retry_delay)
                await self.scrape_prefixes(retries)
            if retries == 0:
                self.fatal_error = True
        except aiohttp.client_exceptions.ServerDisconnectedError:
            retries -= 1
            self.log.append(str(dt.datetime.today()) + ' ERROR: DISCONNECTED FROM SERVER; SCRAPE_PREFIXES(); '
                            'RETRYING; ' + str(retries) + ' TRIES REMAINING')
            if retries != 0:
                await asyncio.sleep(self.retry_delay)
                await self.scrape_prefixes(retries)
            if retries == 0:
                self.fatal_error = True

    # Run scrape_prefixes()
    def run_scrape_prefixes(self, retries=3):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.scrape_prefixes(retries))
        loop.run_until_complete(future)

    # Async get html for course and section pages
    async def get_raw_html(self, retries=3):
        try:
            course_url = self.base_url + 'bwckctlg.p_display_courses?term_in={}&call_proc_in=&sel_subj=dummy' \
                '&sel_levl=dummy&sel_schd=dummy&sel_coll=dummy&sel_divs=dummy&sel_dept=dummy&sel_attr=dummy' \
                '&sel_subj=' + self.subj_str + '&sel_crse_strt=&sel_crse_end=&sel_title=&sel_levl=US&sel_schd=%' \
                '&sel_coll=%&sel_divs=%&sel_from_cred=&sel_to_cred=&sel_attr=%'
            section_url = self.base_url + 'bwckschd.p_get_crse_unsec?term_in={}&sel_subj=dummy&sel_day=dummy' \
                '&sel_schd=dummy&sel_insm=dummy&sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy' \
                '&sel_ptrm=dummy&sel_attr=dummy&sel_subj=' + self.subj_str + '&sel_crse=&sel_title=' \
                '&sel_schd=%25&sel_from_cred=&sel_to_cred=&sel_camp=%25&sel_levl=%25&sel_ptrm=%25' \
                '&sel_attr=%25&begin_hh=0&begin_mi=0&begin_ap=a&end_hh=0&end_mi=0&end_ap=a'
            tasks = []
            async with ClientSession() as session:
                for term in self.terms:
                    tasks.append(asyncio.ensure_future(fetch(course_url.format(term), session)))
                    tasks.append(asyncio.ensure_future(fetch(section_url.format(term), session)))
                responses = await asyncio.gather(*tasks)
                self.raw_courses = []
                self.raw_sections = []
                for i, response in enumerate(responses):
                    if i % 2 == 0:
                        self.raw_courses.append(response)
                    else:
                        self.raw_sections.append(response)
        except aiohttp.client_exceptions.ServerDisconnectedError:
            retries -= 1
            self.log.append(str(dt.datetime.today()) + ' ERROR: DISCONNECTED FROM SERVER; GET_RAW_HTML(); '
                            'RETRYING; ' + str(retries) + ' TRIES REMAINING')
            if retries != 0:
                await asyncio.sleep(self.retry_delay)
                await self.get_raw_html(retries)
            if retries == 0:
                self.fatal_error = True

    # Run get_raw_html()
    def run_get_raw_html(self, retries=3):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.get_raw_html(retries))
        loop.run_until_complete(future)

    # Parse course data returned by get_raw_html()
    def parse_courses(self):
        try:
            self.courses = []
            for index, course in enumerate(self.raw_courses):
                soup = BeautifulSoup(course, 'lxml')
                title = soup.select('.pagebodydiv > .datadisplaytable > tr > .nttitle > a')
                attributes = soup.select('.pagebodydiv > .datadisplaytable > tr > .ntdefault')
                if len(title) != len(attributes):
                    self.log.append(str(dt.datetime.today()) + ' ERROR: LEN(COURSE TITLES) DID NOT MATCH '
                                    'LEN(COURSE ATTRIBUTES); RAW_COURSE INDEX ' + str(index))
                    self.fatal_error = True
                for i, item in enumerate(attributes):
                    credit_str = item.find_all(text=re.compile('Credit hours'))
                    if len(credit_str) > 0:
                        credit_str = credit_str[0].replace('Credit hours', '').strip()
                        if len(credit_str.split()) > 1:
                            credit_str = credit_str.split()[0] + ' - ' + credit_str.split()[-1]
                    else:
                        credit_str = 'TBA'
                    attributes[i] = credit_str
                for i, item in enumerate(title):
                    name_str = item.next.split()
                    subject = name_str[0]
                    number = name_str[1]
                    full_name = ' '.join(item.next.split(' - ', 1)[1].replace('Hybrid', ' ').replace('INTL', ' ')
                                         .replace('-', ' ').split()).strip()
                    self.courses.append([subject,
                                         number,
                                         full_name,
                                         str(self.terms[index]),
                                         attributes[i]])
        except:
            self.log.append(str(dt.datetime.today()) + ' ERROR: PARSE FAILED; PARSE_COURSES()')
            self.fatal_error = True

    # Parse section data returned by get_raw_html()
    def parse_sections(self):
        try:
            self.sections = []
            self.times = []
            for index, section in enumerate(self.raw_sections):
                cursor = len(self.sections)
                soup = BeautifulSoup(section, 'lxml')
                title = soup.select('.ddtitle a')
                # 0:CRN, 1:Subject, 2:Number, 3:Section, 4:Term, 5:Hybrid (y/n),
                # 6:International (y/n), 7:Credits, 8:Instructors []
                for i, item in enumerate(title):
                    hybrid = 'N'
                    intl = 'N'
                    info = item.next.rsplit('-', 3)
                    if 'INTL' in info[-4]:
                        intl = 'Y'
                    if 'Hybrid' in info[-4]:
                        hybrid = 'Y'
                    # Credit, instructor data appended when scraping attributes associated with corresponding index
                    self.sections.append([info[-3].strip(),
                                         info[-2].split()[0].strip(),
                                         info[-2].split()[1].strip(),
                                         info[-1].strip(),
                                         str(self.terms[index]),
                                         hybrid,
                                         intl])
                attributes = soup.select(".pagebodydiv > .datadisplaytable > tr > .dddefault")
                if len(title) != len(attributes):
                    self.log.append(str(dt.datetime.today()) + ' ERROR: LEN(SECTION TITLES) DID NOT MATCH '
                                    'LEN(SECTION ATTRIBUTES); RAW_SECTION INDEX ' + str(index))
                for i, item in enumerate(attributes):
                    # Get and format section credits
                    credit_str = item.find_all(text=re.compile('Credits'))
                    if len(credit_str) > 0:
                        credit_str = credit_str[0].replace('Credits', '').strip()
                        if len(credit_str.split()) > 1:
                            credit_str = credit_str.split()[0] + ' - ' + credit_str.split()[-1]
                    else:
                        credit_str = 'TBA'
                    self.sections[i + cursor].append(credit_str)
                    # Format and compile section times and append instructors to section data
                    times = item.select('.datadisplaytable > tr > .dddefault')
                    # 0:CRN, 1:Times [start, end], 2:Days, 3:Location,
                    # 4:Effective dates [start, end], 5:Term 6:Instructors []
                    if len(times) > 0:
                        for row in range(0, int(len(times) / 7)):
                            offset = row * 7
                            hours = times[1 + offset].get_text().split(' - ', 1)
                            days = times[2 + offset].get_text()
                            if days.strip() in ('', 'TBA'):
                                days = ['TBA']
                            location = times[3 + offset].get_text()
                            dates = times[4 + offset].get_text().split(' - ', 1)
                            instructors = ' '.join(times[6 + offset].get_text().replace('.', '').split()).split(', ')
                            for day in days:
                                self.times.append([self.sections[i + cursor][0],
                                                   hours,
                                                   day,
                                                   location,
                                                   dates,
                                                   str(self.terms[index]),
                                                   instructors])
                        self.sections[i + cursor].append(' '.join(times[6].get_text()
                                                                  .replace('.', '').split()).split(', '))
                    else:
                        self.times.append([self.sections[i + cursor][0],
                                          ['TBA'],
                                           'TBA',
                                           'TBA',
                                          ['TBA'],
                                          str(self.terms[index]),
                                          ['TBA']])
                        self.sections[i + cursor].append(['TBA'])
        except:
            self.log.append(str(dt.datetime.today()) + ' ERROR: PARSE FAILED; PARSE_SECTIONS()')
            self.fatal_error = True

    async def get_ratings(self, retries=3):
        try:
            url = 'https://search-a.akamaihd.net/typeahead/suggest/?rows=1&defType=edismax' \
                  '&siteName=rmp&fl=*&q=schoolid_s%3A12000+AND+{}'
            tasks = []
            name_dict = {}
            kill_counter = 50
            for section in self.sections:
                name = section[8][0]
                if name not in name_dict:
                    formatted_name = name.split()
                    if len(formatted_name) > 1:
                        formatted_name = formatted_name[0] + '+' + formatted_name[-2]
                    else:
                        formatted_name = formatted_name[0]
                    name_dict[name] = formatted_name
            async with ClientSession() as session:
                for key in name_dict:
                    task = asyncio.ensure_future(fetch(url.format(name_dict[key]), session))
                    tasks.append(task)
                ratings = await asyncio.gather(*tasks)
                self.ratings = []
                for i, key in enumerate(name_dict):
                    if kill_counter > 0:
                        try:
                            rating = json.loads(ratings[i])
                        except ValueError:
                            kill_counter -= 1
                            try:
                                self.log.append(str(dt.datetime.today()) + ' ERROR: COULD NOT PARSE JSON; '
                                                'GET_RATINGS() NAME INDEX ' + str(i) + '; ATTEMPTING TO CORRECT')
                                ratings[i] = rs.get(url.format(name_dict[key])).text
                                rating = json.loads(ratings[i])
                            except ValueError:
                                self.log.append(' ERROR: JSON CORRECTION FAILED; GET_RATINGS() NAME INDEX ' + str(i) +
                                                '; VALUE REPLACED WITH {"response":{"numFound":0}}')
                                rating = json.loads('{"response":{"numFound":0}}')
                        self.ratings.append([key, rating])
                    else:
                        self.log.append(str(dt.datetime.today()) + ' ERROR: FAILED RATINGS EXCEEDED ' +
                                        str(kill_counter) + '; GET_RATINGS()')
                        self.fatal_error = True
                        break
        except aiohttp.client_exceptions.ServerDisconnectedError:
            retries -= 1
            self.log.append(str(dt.datetime.today()) + ' ERROR: DISCONNECTED FROM SERVER; GET_RATINGS(); RETRYING; ' +
                            str(retries) + ' TRIES REMAINING')
            if retries != 0:
                await asyncio.sleep(self.retry_delay)
                await self.get_ratings(retries)
            if retries == 0:
                self.fatal_error = True

    def run_get_ratings(self, retries=3):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.get_ratings(retries))
        loop.run_until_complete(future)

    def write_all(self):
        stage = ''
        try:
            print("STARTING DATABASE")
            # TODO - Remove init_db call and method, post-integration
            init_db()
            stage = 'COURSES'
            print(stage)
            Database.drop("courses")
            Database.create("courses",
                            "subject text, number text, title text, term text, credits text",
                            "subject, number, term")
            for item in self.courses:
                data = [item[0], item[1], item[2], item[3], item[4]]
                Database.insert("courses",
                                "subject, number, title, term, credits",
                                "%s, %s, %s, %s, %s",
                                data)
            stage = 'SECTIONS'
            print(stage)
            Database.drop("sections")
            Database.create("sections",
                            "crn text, subject text, number text, section text, term text, "
                            "hybrid text, intl text, credits text, instructor text, faculty text",
                            "crn, term")
            for item in self.sections:
                data = [item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8][0],
                        ", ".join(item[8])]
                Database.insert("sections",
                                "crn, subject, number, section, term, hybrid, intl, credits, instructor, faculty",
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
                                data)
            stage = 'TIMES'
            print(stage)
            Database.drop("times")
            Database.create("times",
                            "crn text, raw_start_time text, raw_end_time text, start_time text, end_time text, "
                            "day text, location text, raw_start_date text, raw_end_date text, "
                            "start_date text, end_date text, term text, faculty text",
                            "crn, start_time, end_time, day, term")
            for item in self.times:
                data = [item[0], item[1][0], item[1][-1], self.process_time(item[1][0]),
                        self.process_time(item[1][-1]), item[2], item[3], item[4][0], item[4][-1],
                        self.process_date(item[4][0]), self.process_date(item[4][-1]), item[5],
                        ", ".join(item[6])]
                Database.insert("times",
                                "crn, raw_start_time, raw_end_time, start_time, end_time, day, location, "
                                "raw_start_date, raw_end_date, start_date, end_date, term, faculty",
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
                                data)
            stage = 'RATINGS'
            print(stage)
            Database.drop("ratings")
            Database.create("ratings",
                            "instructor text, id text, first_name text, last_name text, full_name text, "
                            "average_rating text, rating_count text, easy_score text, clarity_score text, "
                            "helpful_score text, hot_score text",
                            "instructor")
            for item in self.ratings:
                flag = True
                if item[1]['response']['numFound'] > 0:
                    if item[1]['response']['docs'][0]['total_number_of_ratings_i'] > 0:
                        raw_data = item[1]['response']['docs'][0]
                        data = [item[0], raw_data['pk_id'], raw_data['teacherfirstname_t'], raw_data['teacherlastname_t'],
                                raw_data['teacherfullname_s'], raw_data['averageratingscore_rf'],
                                raw_data['total_number_of_ratings_i'], raw_data['averageeasyscore_rf'],
                                raw_data['averageclarityscore_rf'], raw_data['averagehelpfulscore_rf'],
                                raw_data['averagehotscore_rf']]
                        flag = False
                if flag:
                    data = [item[0], None, None, None, None, None, None, None, None, None, None]
                Database.insert("ratings",
                                "instructor, id, first_name, last_name, full_name, average_rating, rating_count, "
                                "easy_score, clarity_score, helpful_score, hot_score",
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
                                data)
        except:
            self.log.append(str(dt.datetime.today()) + ' ERROR: WRITE ABORTED; ' + stage + '; WRITE_ALL(); '
                            'ROLLBACK INITIATED')
            self.fatal_error = True
            Database.rollback()
        if not self.fatal_error:
            Database.commit()

    @staticmethod
    def process_time(time_string):
        if time_string.strip() == 'TBA':
            return 'TBA'
        else:
            time_string = time_string.strip().split(':')
            suffix = time_string[1][-2].strip()
            hour = int(time_string[0])
            if suffix in ('p', 'P') and hour != 12:
                time_string[0] = str(hour + 12)
            elif suffix in ('a', 'A') and hour == 12:
                time_string[0] = '00'
            return (time_string[0] + time_string[1].strip()[:2]).zfill(4)

    @staticmethod
    def process_date(date_string):
        order = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06', 'JUL': '07',
                 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}
        if date_string.strip() == 'TBA':
            return 'TBA'
        else:
            date_string = date_string.strip().split(',')
            date_string[0] = date_string[0].split()
            month = date_string[0][0].strip().upper()[0:3]
            day = date_string[0][1].strip()[0:2]
            year = date_string[1].strip()[0:4]
            if month in order:
                month = order[month]
            else:
                month = '99'
            if len(day) != 2:
                day = day.zfill(2)
            return year + month + day

    def do_all(self):
        print("STARTING RUN...")
        start = time.clock()
        print("SCRAPING TERMS")
        self.scrape_terms()
        print("SCRAPING PREFIXES")
        self.run_scrape_prefixes(3)
        print("GETTING RAW HTML")
        self.run_get_raw_html(3)
        print("PARSING COURSES")
        self.parse_courses()
        print("PARSING SECTIONS")
        self.parse_sections()
        print("GETTING RATING JSON")
        self.run_get_ratings(3)
        if not self.fatal_error:
            print("WRITING TO DATABASE")
            self.write_all()
        print(time.clock() - start)
        if self.fatal_error:
            print("AN ERROR HAS OCCURRED:")
            for error in self.log:
                print(error)

    # TODO - Kill me
    def debug_do_all(self):
        print("STARTING RUN...")
        start = time.clock()
        print("SCRAPING TERMS")
        self.scrape_terms()
        print("SCRAPING PREFIXES")
        self.run_scrape_prefixes(3)
        print("GETTING RAW HTML")
        self.run_get_raw_html(3)
        print("PARSING COURSES")
        self.parse_courses()
        print("PARSING SECTIONS")
        self.parse_sections()
        print("GETTING RATING JSON")
        self.run_get_ratings(3)
        print(time.clock() - start)
        if self.fatal_error:
            print("AN ERROR HAS OCCURRED:")
            for error in self.log:
                print(error)