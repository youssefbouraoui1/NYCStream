import { useState } from "react"



const Pagination = ({datalength,rows,currentpage,UserData,handlenextclick,handlpreviousclicK}) => {
    
    

    

    

    return(
        <div className="flex ">
        <button type="submit" className="p-2 border border-blue-400 rounded" disabled={currentpage===1} onClick={()=>handlpreviousclicK()}>previous</button>
        <h1 className="ml-4">{currentpage}</h1>
        <button type="submit" disabled={currentpage===Math.ceil(datalength/rows)} onClick={()=>handlenextclick()}>next</button>
        </div>
    )
        
    
}

export default Pagination;