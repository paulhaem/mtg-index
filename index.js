const fs = require('fs')
const readline = require('readline')
const stream = require('stream')
const StreamArray = require('stream-json/utils/StreamArray')
const jsonStream = StreamArray.make()
const request = require('sync-request')

const PATH = './resources/AllSets.json'

let documents = []

jsonStream.output.on('data', function ({index, value}) {

	for (const [key, val] of Object.entries(value)) {
		if(val) {
			val.cards.forEach((card) => {
				let cardJson = {
					setName_txt_en: val.name,
					releaseDate_dt: val.releaseDate,
					artist_txt_en: card.artist,
					cmc_i: card.cmc,
					colors_txt_sort: card.colors,
					multiverseId_i: card.multiverseId,
					name_txt_en: card.name,
					power_i: card.power,
					rarity_txt_en: card.rarity,
					subtypes_txt_sort: card.subtypes,
					text_txt_sort: card.text, 
					toughness_i: card.toughness,
					types_txt_sort: card.types
				}
				if(isNaN(card.power)) cardJson.power_i = -1
				if(isNaN(card.toughness)) cardJson.toughness_i = -1
				if(val.releaseDate.length <= 4) cardJson.releaseDate_dt += '-01-01'
				accumData(cardJson)
			})
		}
	}
})

jsonStream.output.on('end', function () {
	sendData(documents)
    documents = []
	console.log('All done')
})

fs.createReadStream(PATH).pipe(jsonStream.input)

// accumulates JSON objects in array until 10k before they are sent
function accumData(postData) {

    documents.push(postData)
    if(documents.length == 10000){
        // send array of JSON objects to solr server
        console.log('sending')
        sendData(documents)
		documents = []
    }
}

// sends data to solr server
function sendData(postData){

    var clientServerOptions = {
        body: JSON.stringify(postData),
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        }
    }
    // on response from server, log response
    let response = request('POST', 'http://localhost:8983/solr/gettingstarted/update/json/docs?commit=true&overwrite=true', clientServerOptions);
    if (response.statusCode !== 200) {
      throw(response.body)
    } else {
      console.log('sent')
    }
}