import React, { useState, useEffect } from "react";
import "./EmailPopUp.css"; // Import CSS for styling

const EmailPopup = () => {
  const [email, setEmail] = useState("");
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    const hasSubscribed = localStorage.getItem("subscribed");

    // Only show the popup if the user hasn't subscribed yet
    if (!hasSubscribed) {
      // Set a timeout to show the popup after 2 seconds
      const timeoutId = setTimeout(() => {
        setIsOpen(true);
      }, 2000);

      // Cleanup the timeout if the component is unmounted before the timeout completes
      return () => clearTimeout(timeoutId);
    }
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();

    try {
      await fetch("https://0h6oy409mb.execute-api.us-east-2.amazonaws.com/prod/subscribe", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ email }),
      });

      localStorage.setItem("subscribed", "true"); // Prevent popup from showing again
      setIsOpen(false); // Close popup after submission
    } catch (error) {
      console.error("Error submitting email:", error);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="popup-overlay">
      <div className="popup-content">
        <h2>Subscribe</h2>
        <p>Join the 10,000+ receiving free, daily AI prop picks</p>
        <form onSubmit={handleSubmit}>
          <input
            type="email"
            placeholder="Enter your email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />
          <button type="submit" className="submit-btn">Subscribe</button>
        </form>
        <button className="close-btn" onClick={() => setIsOpen(false)}>Close</button>
      </div>
    </div>
  );
};

export default EmailPopup;
