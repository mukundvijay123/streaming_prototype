from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.exceptions import HTTPException
import os
import uvicorn

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/pages/{page_name}", response_class=HTMLResponse)
async def serve_page(request: Request, page_name: str):
    template_path = os.path.join("templates", f"{page_name}.html")

    # Optional: check if the file exists, else return 404
    if not os.path.exists(template_path):
        raise HTTPException(status_code=404, detail="Page not found")

    return templates.TemplateResponse(f"{page_name}.html", {"request": request})


if __name__=="__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=3000, reload=True)