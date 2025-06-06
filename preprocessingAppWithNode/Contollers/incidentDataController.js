const validateIncident = require('../Services/validateData')
const kafkaProducer = require('../Services/kafkaProducer')



export const validateAndSendData = async(req,res) => {
    const data = req.body
    try{
    const valide= validateIncident.validateIncident(data)
    await kafkaProducer.sendMessage(valide);
    res.status(200).json({ message: "Incident accepted and sent to Kafka" });
  } catch (e) {
    res.status(400).json({ error: "Invalid data", details: e.message });
  }
        
}