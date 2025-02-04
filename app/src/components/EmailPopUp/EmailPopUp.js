import React, { useState, useEffect } from "react";
import { useAuth } from "react-oidc-context";
import "./EmailPopUp.css"; // Import CSS for styling

const EmailPopup = () => {
  const [isOpen, setIsOpen] = useState(false);

  const auth = useAuth();

  useEffect(() => {
    const hasSubscribed = localStorage.getItem("subscribed");

    // Only show the popup if the user hasn't subscribed yet
    if (!auth.isAuthenticated && !hasSubscribed) {
      // Set a timeout to show the popup after 2 seconds
      const timeoutId = setTimeout(() => {
        setIsOpen(true);
      }, 2000);

      // Cleanup the timeout if the component is unmounted before the timeout completes
      return () => clearTimeout(timeoutId);
    }
  }, [auth.isAuthenticated]);

  const handleSubmit = () => {
    try {
      localStorage.setItem("subscribed", "true"); // Prevent popup from showing again
      setIsOpen(false); // Close popup after submission
      auth.signinRedirect();
    } catch (error) {
      console.error("Error:", error);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="popup-overlay">
      <div className="popup-content">
        <h2>Subscribe</h2>
        <p>Join the 10,000+ receiving free, daily AI prop picks!</p>
        <button className="popUpSignUpButton" onClick={handleSubmit}>Login or Sign Up Now</button>
        <button className="close-btn" onClick={() => setIsOpen(false)}>Close</button>
      </div>
    </div>
  );
};

export default EmailPopup;
