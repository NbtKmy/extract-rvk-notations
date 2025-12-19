# Extract RVK-notations

This code take an excel file which includes a column "isbn" and through SRU

## Requirement

- **[uv](https://docs.astral.sh/uv/)**

## Usage

- Initialize the project
```
git clone https://github.com/NbtKmy/extract-rvk-notations.git
cd extract-rvk-notations
uv sync
```

- Run the code
```
uv run python main.py -f [path to AN excel file]
# The code accepts only the excel file format
# The result ("result.csv") will be saved in the "data" folder
```


