import openai
import pandas as pd
from datetime import datetime
import backoff
from tqdm import tqdm
import os
from dotenv import load_dotenv
import nltk
from nltk.tokenize import sent_tokenize


load_dotenv()
api_key = os.getenv("API_KEY")
openai.api_key = api_key


def read_excel_to_dicts(input_file_name):
    df = pd.read_excel(input_file_name)
    required_columns = ["ASIN", "item_name.value"]

    if all(col in df.columns for col in required_columns):
        return df.to_dict("records")
    else:
        raise Exception("Missing columns!")


def slice_list(lst, slice_size):
    return [lst[i : i + slice_size] for i in range(0, len(lst), slice_size)]


def create_queries_and_payloads(data_list):
    context = "You are an assistant that receives product titles and creates product description which are 6 sentences long and written in Polish."
    queries = []
    ids = []

    for item in data_list:
        item_id = item["ASIN"]
        item_name = item["item_name.value"]
        ids.append(item_id)
        queries.append({"role": "system", "content": context})
        queries.append({"role": "user", "content": item_name})

    return queries, ids


def get_product_description(payloads):
    responses = []

    for element in tqdm(payloads, desc="getting GPT responses"):
        response = completions_with_backoff(
            model="gpt-3.5-turbo",
            messages=element,
            temperature=0.2,
            max_tokens=500,
            frequency_penalty=1.2,
            presence_penalty=1.1,
        )
        responses.append(response["choices"][0]["message"]["content"])

    return responses


@backoff.on_exception(backoff.expo, openai.error.RateLimitError)
def completions_with_backoff(**kwargs):
    return openai.ChatCompletion.create(**kwargs)


def merge(ids_arr, responses_arr):
    id_column_name = "ASIN"
    response_column_name = "rtip_product_description.value"
    responses_with_ids = [
        {id_column_name: k, response_column_name: v}
        for k, v in zip(ids_arr, responses_arr)
    ]
    return responses_with_ids


def split_text(text):
    lang = "polish"
    return sent_tokenize(text, lang)


def process_result(arr):
    result = []

    for element in arr:
        text = element["rtip_product_description.value"]
        tokenize = split_text(text)

        desc = " ".join(tokenize[:3]) if len(tokenize) >= 6 else "no data"
        blt = (
            [point.rstrip(".!?") for point in tokenize[3:]]
            if len(tokenize) >= 6
            else []
        )

        bullet_points = {
            f"bullet_point#{i+1}.value": point if point else "no data"
            for i, point in enumerate(blt[:10])
        }
        bullet_points.update(
            {f"bullet_point#{i+1}.value": "NULL" for i in range(len(blt), 10)}
        )

        result.append(
            {
                "ASIN": element["ASIN"],
                "sc_vendor_name": "AmazonPl/NM5V9",
                "rtip_product_description.value": desc,
                **bullet_points,
            }
        )

    return result


def create_file(user_name, data):
    pd.io.formats.excel.ExcelFormatter.header_style = None
    now = datetime.now()
    current_date = now.strftime("%m%d%Y")

    file = f"FLEX_ATTRPDB {current_date}_{user_name}.xlsx"
    writer = pd.ExcelWriter(file, engine="xlsxwriter")
    data.to_excel(writer, sheet_name="LPD", index=False, startrow=1)
    worksheet = writer.sheets["LPD"]
    worksheet.write_string(0, 0, "version=1.0.0")
    writer.save()


def main():
    input_file_name = "new_model_test.xlsx"
    user_name = "krolikma"

    list_of_dicts = read_excel_to_dicts(input_file_name)
    queries, ids = create_queries_and_payloads(list_of_dicts)

    slice_size = 2
    payloads = slice_list(queries, slice_size)

    responses = get_product_description(payloads)
    responses_with_ids = merge(ids, responses)

    nltk.download("punkt")

    result = process_result(responses_with_ids)
    output = pd.DataFrame(result)

    create_file(user_name, output)


main()
