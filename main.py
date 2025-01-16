# Python data scraper - take all messages in a Discord channel, and send them to CSV.
# Assumes the format: "something that was said" - name
# Anything that doesn't follow this format is sent to a separate CSV.

# Written by Connor Kamrowski

### IMPORTS ###
import os
import sys
import logging

import discord
from dotenv import load_dotenv

import csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

### CONSTANTS ###
QUOTES_HEADER = ["Quote", "Speaker", "Quoted By"]
NOT_QUOTES_HEADER = ["Message", "Sent By"]

CHANNEL_ID = 1317005831870611528

OUTPUT_FOLDER = "Results/"

QUOTES_FILENAME = OUTPUT_FOLDER + 'Quotes.csv'
NOT_QUOTES_FILENAME = OUTPUT_FOLDER + 'not_quotes.csv'



### CODE ###

def createFiles():
    # Create the quotes csv
    with open(QUOTES_FILENAME, 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(QUOTES_HEADER)
    # Create the not-quotes csv
    with open(NOT_QUOTES_FILENAME, 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(NOT_QUOTES_HEADER)


def parseMessageContent(msg):
    quote = ""
    speaker = ""

    # Parse the quote
    quote_mark_index = msg.find("\"")
    if (quote_mark_index != -1):
        last_quote_mark_index = msg.rfind("\"")+1
        if (quote_mark_index < last_quote_mark_index):
            quote = msg[quote_mark_index+1:last_quote_mark_index-1]
        else:
            print("Quote was invalid.")
            return []
    
    # Parse the speaker
    speaker_index = msg.rfind("-")
    if (quote_mark_index == -1):
        return []
    if (msg[speaker_index+1] == " "):
        speaker_index += 1
    speaker = msg[speaker_index+1:]
    
    speaker = speaker[0].upper() + speaker[1:]
    
    # If both worked, return the results
    return [quote, speaker]


async def generateResults():
    if not os.path.exists(OUTPUT_FOLDER + "Images/"):
        os.makedirs(OUTPUT_FOLDER + "Images/")

    quotes_df = pd.read_csv(QUOTES_FILENAME)
    not_quotes_df = pd.read_csv(NOT_QUOTES_FILENAME)

    global num_good, num_bad
    num_good = quotes_df.shape[0]
    num_bad = not_quotes_df.shape[0]

    print()
    print("*** Analysis of The Quotes ***")
    print("Total number of messages scraped: {0}".format(num_good + num_bad))
    print("Total number of good quotes: {0}".format(num_good))
    print("Total number of invalid quotes: {0}".format(num_bad))
    print()
    
    print("Number of times each person was quoted:")
    print(quotes_df["Speaker"].value_counts())
    print()
    quotes_df.Speaker.value_counts().sort_values().plot(kind = 'barh')
    plt.title("Times people were quoted")
    plt.savefig(OUTPUT_FOLDER + "Images/times_person_was_quoted.png")
    plt.clf()

    print("Number of times each person quoted others:")
    print(quotes_df["Quoted By"].value_counts())
    print()
    quotes_df["Quoted By"].value_counts().sort_values().plot(kind = 'barh')
    plt.title("Times people quoted others")
    plt.savefig(OUTPUT_FOLDER + "Images/times_people_quoted_others.png")
    plt.clf()

    print("Number of times each person yapped:")
    print(not_quotes_df["Sent By"].value_counts())
    print()
    not_quotes_df["Sent By"].value_counts().sort_values().plot(kind = 'barh')
    plt.title("Times people YAPPED")
    plt.savefig(OUTPUT_FOLDER + "Images/yaps_per_person.png")
    plt.clf()

    #AAAAATODO: get charts for: (organized by popularity left->right)
    #   total number of quotes
    
    print("Check output for visual diagrams.")

async def sendResults(channel):
    #Send all images into the channel
    await channel.send(file=discord.File(OUTPUT_FOLDER + "Images/times_person_was_quoted.png"))
    await channel.send(file=discord.File(OUTPUT_FOLDER + "Images/times_people_quoted_others.png"))
    await channel.send(file=discord.File(OUTPUT_FOLDER + "Images/yaps_per_person.png"))
    
    #await channel.send("There were " + num_good + " quotes this year.")
    #await channel.send("People yapped " + num_bad + " time(s).")
    #await channel.send(".")

    print("Done sending messages.")

async def performScrape(client, channel):
    #create the output files for raw data
    createFiles()

    print(f'{client.user} has connected to Discord!')
    
    print("Begin scraping channel with ID: {0}".format(CHANNEL_ID))

    async for message in channel.history(oldest_first=True):
        message_contents = parseMessageContent(message.content)
        # if true, then all parsing was successful
        if (len(message_contents) == 2):
            #AAAAATODO - optimize this: opening each time isn't good
            with open(QUOTES_FILENAME, 'a', newline='', encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                to_write = [message_contents[0], message_contents[1], message.author]
                writer.writerow(to_write)
            
        else:
            with open(NOT_QUOTES_FILENAME, 'a', newline='', encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                to_write = [message.content, message.author]
                writer.writerow(to_write)

    print("Finished scraping channel!")
    
    

def main():
    #load the environment from file
    load_dotenv()
    TOKEN = os.getenv('DISCORD_TOKEN')
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    #set the intents of the bot to default
    intents = discord.Intents.default()
    #allow the bot to see message content in 'intents'
    intents.message_content = True
    #create the bot client
    client = discord.Client(intents=intents)

    #print confirmation when bot connects, then start
    @client.event
    async def on_ready():
        channel = client.get_channel(CHANNEL_ID)
        print("Channel name: {0}".format(channel))
        await performScrape(client, channel)
        await generateResults()
        await sendResults(channel)

    handler = logging.FileHandler(filename=OUTPUT_FOLDER + 'client.log', encoding='utf-8', mode='w')

    #run the client using the loaded token
    client.run(TOKEN, log_handler = handler)
    



if __name__ == "__main__":
    main()