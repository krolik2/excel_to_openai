import openai
import pandas as pd
import re
from datetime import datetime
import backoff
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("API_KEY")
openai.api_key = api_key


input_file_name = "03152023.xlsx"
df = pd.read_excel(input_file_name)
list_of_dict = df.to_dict("records")


def slice_list(list):
    ## we need to slice our list and send it in chunks, in order to avoid hitting length limits and getting messed up responses
    slice_size = 3
    slices = [list[i : i + slice_size] for i in range(0, len(list), slice_size)]
    return slices


queries = []
ids = []


def getTitles(list):
    search_phrase = "Product description for minimum of 6 sentences in Polish for:"
    for item in list:
        item_id = item["ASIN"]
        item_name = item["item_name.value"]
        ids.append(item_id)
        queries.append(f"{search_phrase} {item_name}")


getTitles(list_of_dict)

payloads = slice_list(queries)

response_arr = []


def getProdDescription():
    for element in tqdm(payloads):
        response = completions_with_backoff(
            model="text-davinci-003",
            prompt=element,
            temperature=0.8,
            max_tokens=2000,
            top_p=1.0,
            frequency_penalty=0.8,
            presence_penalty=0,
        )
        response_arr.append(response)


@backoff.on_exception(backoff.expo, openai.error.RateLimitError)
def completions_with_backoff(**kwargs):
    return openai.Completion.create(**kwargs)


getProdDescription()

choices_list = []

for element in response_arr:
    dict(element)
    choices = element["choices"]
    choices_list.append(choices)

text_array = []

for i in choices_list:
    for j in i:
        clean = j["text"]
        text_array.append(re.sub("\n", "", clean))

id_column_name = "ASIN"
response_column_name = "rtip_product_description.value"

result = [{id_column_name: k, response_column_name: v} for k, v in zip(ids, text_array)]

now = datetime.now()
currentTime = now.strftime("%H_%M_%S")

res = pd.DataFrame(result)

output_filename = "products description"

res.to_excel(f"{output_filename} - {currentTime}.xlsx", index=False)
