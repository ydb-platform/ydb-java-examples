import React from 'react';
import axios from "axios";

function App() {
  const baseURL = "http://localhost:8080/api/";
  const [post, setPost] = React.useState(null);
  const [title, setTitle] = React.useState('');
  const [text, setText] = React.useState('');

  const activateLasers = () => {
    //too lazy to do normal serial id ^^
    axios.get(baseURL + `create?id=${Math.floor(Math.random() * 10000)}&title=${title}&text=${text}`).then(r => {
      axios.get(baseURL + 'getAll').then((response) => {
        setPost(response.data);
      });
    });
  }

  const handleTitleChange = (event) => {
    setTitle(event.target.value);
  };

  const handleTextChange = (event) => {
    setText(event.target.value);
  };

  React.useEffect(() => {
    axios.get(baseURL + 'getAll').then((response) => {
      setPost(response.data);
    });
  }, []);

  if (!post) return null;


  return (
    <div>
      <input
        type="text"
        id="title"
        name="title"
        onChange={handleTitleChange}
        value={title}
      />
      <input
        type="text"
        id="text"
        name="text"
        onChange={handleTextChange}
        value={text}
      />
      <button onClick={activateLasers}>
        Add Todo
      </button>
      <h1>All todos</h1>
      {post.map((it) => 
      <div>
        <p>{it.title} - {it.text}</p>
        </div>
      )}
    </div>
  );
}

export default App;
