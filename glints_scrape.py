from curl_cffi import requests
from rich import print
import json
import pandas as pd
import time

# Load cookies from config.json
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
    cookies = config['cookies']

# Base headers (we’ll update 'referer' dynamically)
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

# Base JSON payload
json_data = {
    'operationName': 'searchJobs',
    'variables': {
        'data': {
            'CountryCode': 'ID',
            'lastUpdatedAtRange': 'PAST_24_HOURS',
            'includeExternalJobs': True,
            'searchVariant': 'VARIANT_A',
            'limit': 30,
            'offset': 0,  # Will be updated dynamically
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
    offset = (page - 1) * 30  # Calculate offset based on page number (0, 30, 60, ...)
    print(f"Scraping page {page} (offset: {offset})...")

    # Update the referer header with the current page number
    headers['referer'] = f'https://glints.com/id/opportunities/jobs/explore?country=ID&locationName=All+Cities%2FProvinces&lastUpdated=PAST_24_HOURS&page={page}'

    # Update the offset in the json_data
    json_data['variables']['data']['offset'] = offset

    # Make the request
    response = requests.post('https://glints.com/api/v2/graphql', params=params, cookies=cookies, headers=headers, json=json_data)

    # Check if the request was successful
    if response.status_code == 200:
        try:
            response_data = response.json()
            if response_data is None:
                print(f"Page {page}: No JSON response received.")
                continue

            search_jobs = response_data.get('data', {}).get('searchJobs')
            if not search_jobs:
                print(f"Page {page}: 'searchJobs' not found in response.")
                continue

            jobs = search_jobs.get('jobsInPage', [])
            if not jobs:
                print(f"Page {page} returned no jobs.")
                continue

            # Debugging: Print the first job title to verify uniqueness
            print(f"First job on page {page}: {jobs[0].get('title', 'N/A')}")

            # Extract relevant information for each job
            for job in jobs:
                # Salary handling
                salary_info = "N/A"
                salaries = job.get("salaries")
                if salaries and len(salaries) > 0:
                    salary = salaries[0]
                    min_amount = salary.get("minAmount", "N/A")
                    max_amount = salary.get("maxAmount", "N/A")
                    currency = salary.get("CurrencyCode", "N/A")
                    mode = salary.get("salaryMode", "N/A").lower()
                    salary_info = f"{currency} {min_amount} - {max_amount} per {mode}"

                # Skills handling
                skills = ", ".join([skill["skill"]["name"] for skill in job.get("skills", []) if skill.get("mustHave", False)]) or "N/A"

                # City subdivision handling
                city_subdivision_name = "N/A"
                city_subdivision = job.get("citySubDivision")
                if city_subdivision:
                    city_subdivision_name = city_subdivision.get("name", "N/A")

                # Company and Industry handling with explicit None check
                company = job.get("company")
                industry_name = "N/A"
                if company is not None:  # Explicitly check for None
                    industry = company.get("industry")
                    if industry is not None:
                        industry_name = industry.get("name", "N/A")

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
                    "Industry Name": industry_name,
                    "Company Name": job.get("company", {}).get("name", "N/A"),
                    "When Posted": job.get("createdAt", "N/A")
                }
                job_list.append(job_info)
        except KeyError as e:
            print(f"Error parsing response for page {page}: {e}")
            break
        except ValueError as e:
            print(f"Page {page}: Invalid JSON response - {e}")
            print(f"Raw response: {response.text}")
            break
    else:
        print(f"Failed to fetch page {page}: Status code {response.status_code}")
        print(f"Response text: {response.text}")
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
    print("No data was collected. Check your page selection, cookies, or connection.")