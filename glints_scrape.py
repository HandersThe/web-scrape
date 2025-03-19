from curl_cffi import requests
from rich import print
import json
import pandas as pd
import time

cookies = {
    'device_id': 'ac763852-50ba-4f1b-b775-73e045e1a5dc',
    '_gcl_au': '1.1.492769749.1742262178',
    'sessionFirstTouchPath': '/id/opportunities/jobs/explore',
    '_gcl_gs': '2.1.k1$i1742262175$u113747281',
    '_ga': 'GA1.1.1874627153.1742262178',
    '_tgpc': '95087383-3bd6-5242-8594-0a2ca49c11f2',
    '_fbp': 'fb.1.1742262178805.451810530481791905',
    '_tt_enable_cookie': '1',
    '_ttp': '01JPKEP526HSXJDM09T9CAWS34_.tt.1',
    'airbridge_migration_metadata__taplokerbyglints': '%7B%22version%22%3A%221.10.66%22%7D',
    'ab180ClientId': '650f2b78-a53b-41da-b13d-f0cfba39ed70',
    'airbridge_user': '%7B%22attributes%22%3A%7B%22country_code%22%3A%22ID%22%2C%22role%22%3A%22CANDIDATE%22%2C%22has_whatsapp_number%22%3Afalse%2C%22utm_campaign%22%3A%22ID%7CMarketplace%7CSearch%7CBrand%22%2C%22utm_medium%22%3A%22cpc%22%2C%22utm_source%22%3A%22google%22%7D%7D',
    '_gcl_aw': 'GCL.1742262262.EAIaIQobChMIqsKg0sCSjAMVaqNmAh2EfQ0bEAAYASAAEgLrDPD_BwE',
    'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%22195a6eb131cef6-05306403171b52-26011d51-2073600-195a6eb131d131c%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_utm_source%22%3A%22google%22%2C%22%24latest_utm_medium%22%3A%22cpc%22%2C%22%24latest_utm_campaign%22%3A%22ID%7CMarketplace%7CSearch%7CBrand%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1YTZlYjEzMWNlZjYtMDUzMDY0MDMxNzFiNTItMjYwMTFkNTEtMjA3MzYwMC0xOTVhNmViMTMxZDEzMWMifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%7D',
    'builderSessionId': '148bcca97ed74b8c9e8f573ecd6674fe',
    'sessionLastTouchPath': '/id',
    '_tguatd': 'eyJzYyI6IihkaXJlY3QpIiwiZnRzIjoiKGRpcmVjdCkifQ==',
    '_tgidts': 'eyJzaCI6ImQ0MWQ4Y2Q5OGYwMGIyMDRlOTgwMDk5OGVjZjg0MjdlIiwiY2kiOiI4MmY3OThiMC0xMDE2LTVjYWMtYmIxYS01M2E3NGQ1MDNlYjMiLCJzaSI6IjYxMTBiOTg1LTUxYzQtNWQ1Yy1hZjkxLTk1YzY2MzY3NDQ0MSJ9',
    'sessionIsLastTouch': 'false',
    '_tglksd': 'eyJzIjoiNjExMGI5ODUtNTFjNC01ZDVjLWFmOTEtOTVjNjYzNjc0NDQxIiwic3QiOjE3NDIzNDY4Nzc0NjUsImciOiJFQUlhSVFvYkNoTUlxc0tnMHNDU2pBTVZhcU5tQWgyRWZRMGJFQUFZQVNBQUVnTHJEUERfQndFIiwiZ3QiOjE3NDIyNjIxNzg2NTIsInNvZCI6IihkaXJlY3QpIiwic29kdCI6MTc0MjI2Njg2MDg2NCwic29kcyI6ImMiLCJzb2RzdCI6MTc0MjM0Njg4MTE1Mn0=',
    'traceInfo': '%7B%22expInfo%22%3A%22%22%2C%22requestId%22%3A%22b8a545b6276bff974902974be485b4d1%22%7D',
    'airbridge_touchpoint': '%7B%22channel%22%3A%22glints.com%22%2C%22parameter%22%3A%7B%7D%2C%22generationType%22%3A1224%2C%22url%22%3A%22https%3A//glints.com/id/opportunities/jobs/explore%3Fcountry%3DID%26locationName%3DAll+Cities%252FProvinces%26lastUpdated%3DPAST_24_HOURS%22%2C%22timestamp%22%3A1742346930667%7D',
    'airbridge_session': '%7B%22id%22%3A%22d9d38aad-5eab-4155-a0c8-6147181c25b7%22%2C%22timeout%22%3A1800000%2C%22start%22%3A1742346904339%2C%22end%22%3A1742346930672%7D',
    '_tgsid': 'eyJscGQiOiJ7XCJscHVcIjpcImh0dHBzOi8vZ2xpbnRzLmNvbSUyRmlkXCIsXCJscHRcIjpcIkdsaW50cyUzQSUyMFNpdHVzJTIwTG93b25nYW4lMjBLZXJqYSUyMFRlcmJhaWslMjBkaSUyMEluZG9uZXNpYVwiLFwibHByXCI6XCJcIn0iLCJwcyI6IjgyNzY2MjA3LWQwNmYtNDQxNi05MjBlLWE5OTVhMjAzYWMwMiIsInB2YyI6IjQiLCJzYyI6IjYxMTBiOTg1LTUxYzQtNWQ1Yy1hZjkxLTk1YzY2MzY3NDQ0MTotMSIsImVjIjoiMTkiLCJwdiI6IjEiLCJ0aW0iOiI2MTEwYjk4NS01MWM0LTVkNWMtYWY5MS05NWM2NjM2NzQ0NDE6MTc0MjM0Njg4MTM0MTotMSJ9',
    '_ga_FQ75P4PXDH': 'GS1.1.1742346876.6.1.1742346983.15.0.0',
}

# Base headers (update 'referer' dynamically)
headers = {
    'accept': '*/*',
    'accept-language': 'id',
    'content-type': 'application/json',
    'origin': 'https://glints.com',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'traceparent': '00-43fffad6f6e3ddedc127711ba64711e5-a5d3a7c4c61f00c5-01',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'x-glints-country-code': 'ID',
}

params = {
    'op': 'searchJobs',
}

# Base JSON payload (no need for offset if page is handled via URL)
json_data = {
    'operationName': 'searchJobs',
    'variables': {
        'data': {
            'CountryCode': 'ID',
            'lastUpdatedAtRange': 'PAST_24_HOURS',
            'includeExternalJobs': True,
            'searchVariant': 'VARIANT_A',
            'limit': 30,
            # Removed 'offset' since pagination seems tied to the page parameter in the referer
        },
    },
    'query': 'query searchJobs($data: JobSearchConditionInput!) {\n  searchJobs(data: $data) {\n    jobsInPage {\n      id\n      title\n      workArrangementOption\n      status\n      createdAt\n      updatedAt\n      isActivelyHiring\n      isHot\n      isApplied\n      shouldShowSalary\n      educationLevel\n      type\n      fraudReportFlag\n      salaryEstimate {\n        minAmount\n        maxAmount\n        CurrencyCode\n        __typename\n      }\n      company {\n        ...CompanyFields\n        __typename\n      }\n      citySubDivision {\n        id\n        name\n        __typename\n      }\n      city {\n        ...CityFields\n        __typename\n      }\n      country {\n        ...CountryFields\n        __typename\n      }\n      salaries {\n        ...SalaryFields\n        __typename\n      }\n      location {\n        ...LocationFields\n        __typename\n      }\n      minYearsOfExperience\n      maxYearsOfExperience\n      source\n      type\n      hierarchicalJobCategory {\n        id\n        level\n        name\n        children {\n          name\n          level\n          id\n          __typename\n        }\n        parents {\n          id\n          level\n          name\n          __typename\n        }\n        __typename\n      }\n      skills {\n        skill {\n          id\n          name\n          __typename\n        }\n        mustHave\n        __typename\n      }\n      traceInfo\n      __typename\n    }\n    numberOfJobsCreatedInLast14Days\n    totalJobs\n    expInfo\n    __typename\n  }\n}\n\nfragment CompanyFields on Company {\n  id\n  name\n  logo\n  status\n  isVIP\n  IndustryId\n  industry {\n    id\n    name\n    __typename\n  }\n  verificationTier {\n    type\n    __typename\n  }\n  __typename\n}\n\nfragment CityFields on City {\n  id\n  name\n  __typename\n}\n\nfragment CountryFields on Country {\n  code\n  name\n  __typename\n}\n\nfragment SalaryFields on JobSalary {\n  id\n  salaryType\n  salaryMode\n  maxAmount\n  minAmount\n  CurrencyCode\n  __typename\n}\n\nfragment LocationFields on HierarchicalLocation {\n  id\n  name\n  administrativeLevelName\n  formattedName\n  level\n  slug\n  latitude\n  longitude\n  parents {\n    id\n    name\n    administrativeLevelName\n    formattedName\n    level\n    slug\n    CountryCode: countryCode\n    parents {\n      level\n      formattedName\n      slug\n      __typename\n    }\n    __typename\n  }\n  __typename\n}',
}

# List to hold all job data
job_list = []

# Get user input for page selection
print("Enter the page(s) you want to scrape (e.g., '2' for page 2 only, or '3-5' for pages 3 to 5):")
user_input = input("Your choice: ").strip()

# Parse user input
if '-' in user_input:
    try:
        start_page, end_page = map(int, user_input.split('-'))
        if start_page < 1 or end_page < start_page:
            raise ValueError("Invalid page range. Start page must be >= 1 and less than or equal to end page.")
        pages = range(start_page, end_page + 1)
    except ValueError as e:
        print(f"Error: {e}. Defaulting to page 1 only.")
        pages = [1]
else:
    try:
        page = int(user_input)
        if page < 1:
            raise ValueError("Page number must be >= 1.")
        pages = [page]
    except ValueError as e:
        print(f"Error: {e}. Defaulting to page 1 only.")
        pages = [1]

# Loop through the selected pages
for page in pages:
    print(f"Scraping page {page}...")

    # Update the referer header with the current page number
    headers['referer'] = f'https://glints.com/id/opportunities/jobs/explore?country=ID&locationName=All+Cities%2FProvinces&lastUpdated=PAST_24_HOURS&page={page}'

    # Make the request
    response = requests.post('https://glints.com/api/v2/graphql', params=params, cookies=cookies, headers=headers, json=json_data)

    # Check if the request was successful
    if response.status_code == 200:
        try:
            jobs = response.json()['data']['searchJobs']['jobsInPage']
            if not jobs:  # If jobsInPage is empty, we might have reached the end
                print(f"Page {page} returned no jobs.")
                continue

            # Extract relevant information for each job
            for job in jobs:
                # Salary handling
                salary_info = "N/A"
                if job.get("salaries") and len(job["salaries"]) > 0:
                    salary = job["salaries"][0]
                    min_amount = salary.get("minAmount", "N/A")
                    max_amount = salary.get("maxAmount", "N/A")
                    currency = salary.get("CurrencyCode", "N/A")
                    mode = salary.get("salaryMode", "N/A").lower()
                    salary_info = f"{currency} {min_amount} - {max_amount} per {mode}"

                # Skills handling
                skills = ", ".join([skill["skill"]["name"] for skill in job.get("skills", []) if skill.get("mustHave", False)]) or "N/A"

                # City subdivision handling
                city_subdivision_name = "N/A"
                if job.get("citySubDivision"):
                    city_subdivision_name = job["citySubDivision"].get("name", "N/A")

                job_info = {
                    "Job Name": job.get("title", "N/A"),
                    "Job Type": job.get("type", "N/A"),
                    "Salary": salary_info,
                    "Years of Experience": f"{job.get('minYearsOfExperience', 'N/A')} - {job.get('maxYearsOfExperience', 'N/A')} years",
                    "Education Level": job.get("educationLevel", "N/A"),
                    "Work Arrangement": job.get("workArrangementOption", "N/A"),
                    "Job Category": job.get("hierarchicalJobCategory", {}).get("name", "N/A"),
                    "Required Skills": skills,
                    "City Name": job.get("city", {}).get("name", "N/A"),
                    "City Subdivision Name": city_subdivision_name,
                    "Industry Name": job.get("company", {}).get("industry", {}).get("name", "N/A"),
                    "Company Name": job.get("company", {}).get("name", "N/A"),
                    "When Posted": job.get("createdAt", "N/A")
                }
                job_list.append(job_info)
        except KeyError as e:
            print(f"Error parsing response for page {page}: {e}")
            break
    else:
        print(f"Failed to fetch page {page}: Status code {response.status_code}")
        break

    # Add a delay to avoid rate limiting (e.g., 2 seconds)
    time.sleep(2)

# Check if any data was collected
if job_list:
    # Create a Pandas DataFrame
    df = pd.DataFrame(job_list)

    # Save to CSV file
    df.to_csv("glints_jobs_data.csv", index=False)
    print("Data has been saved to 'glints_jobs_data.csv'")

    # Save the extracted data to a JSON file
    with open("glints_jobs_data.json", "w") as json_file:
        json.dump(job_list, json_file, indent=4)
    print("Data has been saved to 'glints_jobs_data.json'")
else:
    print("No data was collected. Check your page selection or connection.")