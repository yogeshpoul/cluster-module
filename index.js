const express=require("express");
const app=express();

// app.use(express.json());

app.get('/',(req,res)=>{
    console.log("listingin");
})
app.post('/',(req,res)=>{
    console.log("listingin",req);
    res.json(req);
})
app.listen(3000);
